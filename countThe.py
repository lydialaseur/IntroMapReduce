from mrjob.job import MRJob


class TheCounter(MRJob):

    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        occurences = 0
        if "the" in words:
            occurences = words.count("the")

        yield "occurences_of_the", occurences

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    TheCounter.run()
