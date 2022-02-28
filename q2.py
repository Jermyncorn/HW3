from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MapReduceQ2(MRJob):
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
            yield parts[7],parts[3]+"*"+parts[5]

    def reducer(self, word, values):
        totalearning = 0.0
        totalquantity = 0
        for val in values:
            split_str = val.split("*")
            totalquantity = totalquantity + int(split_str[0])
            totalearning = totalearning + float(split_str[1])
        yield word, (totalquantity, totalearning)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == '__main__':
    MapReduceQ2.run()
