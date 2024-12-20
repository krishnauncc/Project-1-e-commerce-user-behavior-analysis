from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class ProductRevenueAnalysis(MRJob):

    def configure_args(self):
        super(ProductRevenueAnalysis, self).configure_args()
        self.add_file_arg('--products')  # Add products.csv as an additional file

    def mapper_init(self):
        # Load product data from the CSV file into a dictionary
        self.products = {}
        with open(self.options.products, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.products[row['ProductID']] = {
                    'ProductName': row['ProductName'],
                    'ProductCategory': row['ProductCategory'],
                    'Price': float(row['Price'])
                }

    def mapper(self, _, line):
        # Parse each transaction row
        fields = line.split(',')
        if fields[0] == "TransactionID":
            return  # Skip the header row in transactions.csv

        product_id = fields[3]
        revenue_generated = float(fields[5])

        # Emit (ProductID, (ProductCategory, ProductName, RevenueGenerated))
        if product_id in self.products:
            product_info = self.products[product_id]
            yield product_id, (product_info['ProductCategory'], product_info['ProductName'], revenue_generated)

    def reducer_total_revenue(self, product_id, values):
        # Aggregate the total revenue for each product
        total_revenue = 0
        product_category = None
        product_name = None

        for value in values:
            product_category = value[0]
            product_name = value[1]
            total_revenue += value[2]

        # Emit total revenue and product details for further processing
        yield product_category, (product_name, product_id, total_revenue)

    def reducer_average_and_top_products(self, category, values):
        # Calculate average revenue and identify top 3 products for each category
        products = []
        total_revenue = 0
        count = 0

        for value in values:
            product_name, product_id, revenue = value
            products.append((product_name, product_id, revenue))
            total_revenue += revenue
            count += 1

        average_revenue = total_revenue / count if count > 0 else 0

        # Sort products by revenue in descending order to find the top 3
        top_products = sorted(products, key=lambda x: x[2], reverse=True)[:3]

        # Yield category-level results
        yield category, {
            'AverageRevenue': average_revenue,
            'Top3Products': top_products
        }

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer_total_revenue),
            MRStep(reducer=self.reducer_average_and_top_products)
        ]

if _name_ == '_main_':
    ProductRevenueAnalysis.run()