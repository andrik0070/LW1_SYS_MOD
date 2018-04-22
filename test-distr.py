import distributions
import  csv

with open('exponential-distr.csv', 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL, dialect='excel')
    numbers = []
    for i in range(1, 100000):
        writer.writerow([distributions.exponential(0.2)])


