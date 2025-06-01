from prefect import flow
from tasks.ringo_transform import transform_ringo

@flow(name="ringo_flow")
def ringo_flow():
    raw_data = []  # TODO: fetch from 
    df = transform_ringo(raw_data)
    # TODO: write df to PostgreSQL

if __name__ == "__main__":
    ringo_flow()
