from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceExample2(MRJob):
    def steps(self, ):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        (movie_id, user_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MapReduceExample2.run()