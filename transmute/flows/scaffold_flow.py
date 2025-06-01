from prefect import flow
from tasks.scaffold_transform import transform_scaffold

@flow(name="scaffold_flow")
def scaffold_flow():
    raw_data = []  # TODO: fetch from 
    df = transform_scaffold(raw_data)
    # TODO: write df to PostgreSQL

if __name__ == "__main__":
    scaffold_flow()
