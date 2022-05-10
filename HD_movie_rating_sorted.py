from functools import reduce
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdown(MRJob):
    def steps(self):
        return[
            MRStep(
                mapper = self.mapper_get_ratings,
                combiner = self.combine_movie_rating_and_add,
                reducer = self.reducer_count_ratings
            ),
            MRStep(
                reducer = self.reducer_sort_counts
            )
        ]

    # The mapper() method takes a key and a value as args
    # in this case, the key is ignored and a single line of text input is the value) 
    # and yields as many key-value pairs as it likes.
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # The Combiner
    # takes the a key, and subset the values for that key and return a key-value pairs
    # this will optimize the run after running the mapper method
    # can be used to decrease the total data transfer.
    def combine_movie_rating_and_add(self, key, values):
        yield key, sum(values)


    # The reducer method takes all the key-values and counts all the values 
    # and add them together toi the same key which then show the total
    # value for each rating key
    # The sum will add up and give total 
    def reducer_count_ratings (self, key, values):
        yield None, (sum(values), key)

    #Sorter
    #reverse true to show descending order of the movies.
    def reducer_sort_counts(self, _, values):
        for count, key in sorted(values, reverse=True):
            yield int(key), count


if __name__ == '__main__':
    MoviesBreakdown.run()

    ##example code: https://stackoverflow.com/questions/53707962/mrjob-sort-reducer-output