from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingJob(MRJob):
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

    # The mapper() - for 6 points
    # method takes a key and a value as args
    # so, Get the id's and maps all the movies.
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

   
    # The reducer - for 6 points
    # method takes all the key-values and counts all the values 
    # and add them together to the same key which then show the total
    # value for each rating key
    # The sum will add up and give total 
    def reducer_count_ratings (self, movieID, rating):
        yield None, (sum(rating), movieID)


    # The Combiner - for 8 points
    # takes the a key, and subset the values for that key and return a key-value pairs
    # this will optimize the run after running the mapper method
    # can be used to decrease the total data transfer.
    def combine_movie_rating_and_add(self, movieID, rating):
        yield movieID, sum(rating)

    # Sorter - For 8 points
    # sort the movies based on the movie rating.
    # the Reverse parameter is set to true to show descending order of the movies.
    def reducer_sort_counts(self, _, values):
        for count, movieID in sorted(values, reverse=True):
            yield int(movieID), count


if __name__ == '__main__':
    RatingJob.run()

    ##example code: https://stackoverflow.com/questions/53707962/mrjob-sort-reducer-output