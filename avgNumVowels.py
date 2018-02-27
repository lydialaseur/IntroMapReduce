from mrjob.job import MRJob
from collections import Counter

class AverageVowelsPerLength(MRJob):

    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        for w in words:
            letter_counts = Counter(w)
            num_vowels = (  letter_counts['a'] +
                            letter_counts['e'] +
                            letter_counts['i'] +
                            letter_counts['o'] +
                            letter_counts['u'] )
            yield len(w), num_vowels

    def reducer(self, key, values):
        num_words = 0
        total_num_vowels = 0
        for v in values:
            num_words += 1
            total_num_vowels += v

        yield key, total_num_vowels/num_words


if __name__ == '__main__':
    AverageVowelsPerLength.run()
