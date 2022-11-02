### import libraries 
from mrjob.job import MRJob
import psycopg2


### define mrjob
class MRFilter(MRJob):

    # define the connection
    def mapper_init(self):
        self.conn = psycopg2.connect(database= 'database', user ='postgres', password='mekanika', host='localhost', port='5432')

    # filter the data
    def mapper(self, _, line):
        self.cur = self.conn.cursor()
        item = line.strip().split(',')    
        if item[3] == 'Price':
            pass
        elif int(item[3]) > 10:
            self.cur.execute("insert into product (product_id, product_name, product_category, price) values(%s, %s, %s, %s)", (item[0], item[1], item[2], item[3]))
            # yield None, line

    # commit the connection
    def mapper_final(self):
        self.conn.commit()
        self.conn.close()

### run the mrjob
if __name__ == '__main__':
   MRFilter.run()