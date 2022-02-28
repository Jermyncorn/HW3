from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import csv

class MapReduceQ3(MRJob):
    def mapper_init(self):
        self.country_earned = defaultdict(float)
    
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
            if parts[7] in self.country_earned:
                self.country_earned[parts[7]] = self.country_earned[parts[7]] + float(parts[5])
            else:
                self.country_earned[parts[7]] = float(parts[5])
    
    def mapper_final(self):
        for country, value in self.country_earned.items():
            yield country, value

    def reducer(self, word, values):
        yield word, sum(values)

if __name__ == '__main__':
    MapReduceQ3.run()
