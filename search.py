from datetime import datetime, date, time

import codecs
import collections
import logging
import os
import os.path
import Queue
import sys

from concurrent import futures
import regex

import lucene

if not lucene.getVMEnv():
    lucene.initVM(maxheap='1G')

from lupyne import engine
from lupyne.engine.documents import DateTimeField, NumericField
from org.apache.lucene import analysis, document, index, queryparser, search, store, util
from whoosh.qparser.dateparse import DateParserPlugin

from guppy import hpy
import prctl
import pytz

import config

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)


class LineMatcher(object):
    LINE_REGEX_PRE = ur"\[(?P<hh>\d\d):(?P<mm>\d\d):(?P<ss>\d\d)\] "
    LINE_REGEX_STARS = LINE_REGEX_PRE + ur"\*\*\* "
    LINE_REGEX_NORMAL = LINE_REGEX_PRE + ur"<(?P<author>[^>]*)> (?P<content>.*)"
    LINE_REGEX_ACTION = LINE_REGEX_PRE + ur"\* (?P<author>[^ ]*) (?P<content>.*)"
    LINE_REGEX_MODE = LINE_REGEX_STARS + ur"(?P<author>[^ ]*) sets mode.*"
    LINE_REGEX_MOVEMENT = LINE_REGEX_STARS + ur"(?P<action>Joins|Parts|Quits): (?P<author>[^ ]*).*" # I don't think we actually care about this info too much.
    LINE_REGEX_NICKCHANGE = LINE_REGEX_STARS + ur"(?P<author>[^ ]*) is now known as (?P<new_name>.*)"
    LINE_REGEX_KICK = LINE_REGEX_STARS + ur"(?P<author>[^ ]*) was kicked.*"
    LINE_REGEX_NOTICE = LINE_REGEX_PRE + ur"-(?P<author>.*)- (?P<content>.*)"
    LINE_REGEX_CONNECT = LINE_REGEX_PRE + ur"(Connected to|Disconnected from) IRC.*"
    LINE_REGEX_ZNC_INFO = LINE_REGEX_PRE + ur"Broadcast: .*"
    LINE_REGEX_TOPIC_CHANGE = LINE_REGEX_STARS + ur"(?P<author>[^ ]*) changes topic to (?P<topic>.*)"

    LINE_REGEXES = [
        LINE_REGEX_NORMAL,
        LINE_REGEX_ACTION,
        LINE_REGEX_MODE,
        LINE_REGEX_MOVEMENT,
        LINE_REGEX_NICKCHANGE,
        LINE_REGEX_KICK,
        LINE_REGEX_NOTICE,
        LINE_REGEX_CONNECT,
        LINE_REGEX_ZNC_INFO,
        LINE_REGEX_TOPIC_CHANGE,
    ]

    def __init__(self):
        self._regexes = map(regex.compile, self.LINE_REGEXES)

    def match(self, line):
        for regexp in self._regexes:
            m = regexp.match(line)
            if m is not None:
                return m.groupdict()
        return


class LogWalker(object):
    USERS = "users"
    MODDATA = "moddata"
    LOG = "log"

    FILE_REGEX = ur"(default_)?(?P<target>.*)_(?P<year>\d{4})(?P<month>\d\d)(?P<day>\d\d)\.log"

    def __init__(self, indexer):
        self.regex_pool = self.make_pool(max_workers=2)
        self.index_pool = self.make_pool(max_workers=4)

        self.indexer = indexer
        self.line_matcher = LineMatcher()
        self.timezone = pytz.timezone(config.LOG_TIMEZONE)
        self.errors = []

    def make_pool(self, max_workers=4, queue_size=8):
        pool = futures.ThreadPoolExecutor(max_workers=max_workers)
        pool._work_queue = Queue.Queue(maxsize=queue_size)
        return pool

    def attach_and_index(self, *args, **kwargs):
        prctl.set_name("index worker")
        lucene.getVMEnv().attachCurrentThread()
        self.indexer.add(*args, **kwargs)

    def walk_log(self, network_name, log_path, log_file, log_meta):

        log_file_full = os.path.join(log_path, log_file)
        print u"Parsing {0}/{1}".format(network_name, log_file)
        prctl.set_name("regex worker")

        with codecs.open(log_file_full, 'rb', 'utf-8', 'replace') as log:
            # Somehow a \x1d got interpreted as a newline character. What?
            log_split = log.read().split("\n")

            for line_no, line in enumerate(log_split):

                # ???
                if line == u'':
                    continue

                line_data = self.line_matcher.match(line)

                if line_data is None:
                    self.errors.append("Line {0}: ``{1!r}'' at {2} {3}".format(line_no, line, network_name, log_meta))
                    print self.errors[-1]
                    continue

                time = datetime(
                    int(log_meta['year']),
                    int(log_meta['month']),
                    int(log_meta['day']),
                    int(line_data['hh']),
                    int(line_data['mm']),
                    int(line_data['ss']),
                )

                self.index_pool.submit(
                    self.attach_and_index,
                    line_no=line_no + 1,
                    network=network_name,
                    channel=log_meta['target'],
                    author=line_data.get('author', ''),
                    time=time,
                    content=line_data.get('content', '')
                )

    def walk_network(self, network_name, dir):
        log_path = os.path.join(dir, self.MODDATA, self.LOG)

        if not os.path.exists(log_path):
            return

        files = os.listdir(log_path)

        log_files = [regex.match(self.FILE_REGEX, fn) for fn in files]
        log_files = [(m.group(0), m.groupdict()) for m in log_files if m is not None]
        log_files.sort()

        for index, (log_file, log_meta) in enumerate(log_files):
            self.regex_pool.submit(self.walk_log, network_name, log_path, log_file, log_meta)

            # print index % 10
            if index % 10 == 0:
                # import pdb; pdb.set_trace()
                pass

    def walk(self):
        """Don't get confused, kids -- we're still using the old ZNC 0.2 paradigm of one user per network."""
        networks_dir = os.path.join(config.LOG_BASE_DIR, self.USERS)

        full_dir = [os.path.join(networks_dir, dir) for dir in os.listdir(networks_dir)]
        full_dir = [dir for dir in full_dir
                    if (not os.path.isfile(dir)) and os.path.exists(os.path.join(dir, self.MODDATA))
                    ]
        networks = [unicode(os.path.basename(dir)) for dir in full_dir]

        networks.sort()
        full_dir.sort()

        for network_name, dir in zip(networks, full_dir):
            self.walk_network(network_name, dir)

        self.indexer.commit()

        if any(self.errors):
            raise Exception("Parsing errors at: {0}".format(self.errors))


class Index(object):
    def __init__(self):
        indexer = engine.Indexer(directory=config.LUCENE_INDEX_DIR)
        indexer.set('line_no', stored=True, cls=engine.NumericField, type=int)
        indexer.set('network', stored=True, tokenized=False)
        indexer.set('channel', stored=True, tokenized=False)
        indexer.set('author', stored=True)
        indexer.set('time', cls=engine.DateTimeField, stored=True)
        indexer.set('content', stored=True)

        self.indexer = indexer

    def do_reindex(self):
        walker = LogWalker(self.indexer)
        walker.walk()

    def search(self, content, time_text=None, **kwargs):
        """The keyword args here are actually fixed. They are:
            network - the name of the network
            channel- the #name of the channel
            author - the author of the line
        """

        # Manually set up the query for the content.
        analyzer = analysis.standard.StandardAnalyzer(util.Version.LUCENE_CURRENT)
        parser = queryparser.classic.QueryParser(util.Version.LUCENE_CURRENT, "content", analyzer)
        queries = [parser.parse(content)]

        for key in ['network', 'channel', 'author']:  # Don't do this, kids
            if key in kwargs:
                terms = map(lambda term: engine.Query.term(key, term), kwargs[key])
                queries.append(engine.Query.any(*terms))

        if time_text:
            # TODO: Move this function outside
            dp = DateParserPlugin()
            range = dp.dateparser.date_from(time_text, dp.basedate)
            queries.append(self.indexer.fields['time'].range(range.start, range.end))

        master = engine.Query.all(*queries)
        print master
        return self.indexer.search(master, sort='time')

    def context_search(self, hit):
        """Run a context search from a hit
        """

        one_way = int(config.SEARCH_CONTEXT_LINES / 2)
        queries = []

        for key in ['network', 'channel']:
            queries.append(engine.Query.term(key, hit[key]))

        start_day, end_day = self.make_range(hit['time'])
        queries.append(self.indexer.fields['time'].range(start_day, end_day, upper=True))

        queries.append(self.indexer.fields['line_no'].range(hit['line_no'] - one_way, hit['line_no'] + one_way, upper=True))

        master = engine.Query.all(*queries)
        print master
        return self.indexer.search(master)

    def make_range(self, timestamp):
        """Generate a day range from a timestamp (for use with line range searches)
        """

        dt = datetime.fromtimestamp(timestamp)
        date = dt.date()

        start = datetime.combine(date, time.min)
        end = datetime.combine(date, time.max)

        return start, end

class LineHit(object):
    formatter = u"[{hh}:{mm}:{ss}] <{author}> {content}".format

    @classmethod
    def format(cls, hit):
        dt = datetime.fromtimestamp(hit['time'])
        hh = str(dt.hour).zfill(2)
        mm = str(dt.minute).zfill(2)
        ss = str(dt.second).zfill(2)

        return cls.formatter(hh=hh, mm=mm, ss=ss, author=hit['author'], content=hit['content'])

Index().do_reindex()
