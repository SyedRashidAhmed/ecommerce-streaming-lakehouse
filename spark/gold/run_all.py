from spark.gold.dim_customer import build_dim_customer
from spark.gold.dim_product import build_dim_product
from spark.gold.fact_orders import build_fact_orders
from spark.gold.gold_aggregates import build_aggregates

def main():
    build_dim_customer()
    build_dim_product()
    build_fact_orders()
    build_aggregates()

if __name__ == "__main__":
    main()
