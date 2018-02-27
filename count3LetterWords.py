from mrjob.job import MRJob


class ThreeLetterCounter(MRJob):

    def mapper(self, _, line):
        words = line.split()
        yield "num_three_letter_words", words

    def reducer(self, key, values):
        total = 0
        for v in values:
            for word in v:
                if len(word) == 3:
                    total += 1
        yield key, total


if __name__ == '__main__':
    ThreeLetterCounter.run()
