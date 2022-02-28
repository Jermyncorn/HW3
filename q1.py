from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MapReduceQ1(MRJob):
    def mapper(self, _, line):
        header = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        firstline = line.split(",")
        if header != firstline:
            reader = csv.reader(line.split(","), delimiter=",")
            parts = list(reader)
            for i in range(len(parts)):
                if len(parts[i]) == 0:
                    parts[i] = ""
                else:
                    parts[i] = parts[i][0].strip()
            yield parts[7],parts[5]

    def reducer(self, word, values):
        total = 0.0
        for val in values:
            total = total + float(val)
        yield word, total

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == '__main__':
    MapReduceQ1.run()
