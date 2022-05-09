from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdown(MRJob):
    def steps(self):
        return[
            MRStep(
                mapper = self.mapper_get_movies,
                reducer = self.reducer_count_movies
            )
        ]

    def mapper_get_movies(self,_,line):
        (movieID, movieName, movieData, link)=line.split(',')
        yield movieID, 4

    def reducer_count_movies(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MoviesBreakdown.run()