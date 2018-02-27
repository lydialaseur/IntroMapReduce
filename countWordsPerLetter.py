from mrjob.job import MRJob


class WordsPerLetterCounter(MRJob):

    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        for w in words:
            begins_with = w[0]
            yield begins_with, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    WordsPerLetterCounter.run()
