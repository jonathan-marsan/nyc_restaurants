
import os

from sodapy import Socrata

from utilities import get_count, list_to_csv

nyc_inspect_source = 'data.cityofnewyork.us'
nyc_inspect_data_id = '9w7m-hzhe'
output_filename = 'output/nyc_inspection_data.csv'
app_token = os.environ['NYC_APP_TOKEN']

def write_nyc_inspection_to_csv(source, id, token, output_csv):
    client = Socrata(source, token)
    count = get_count(client=client, dataset=id)
    nyc_data = client.get(id, content_type="csv", limit = count)
    list_to_csv(list=nyc_data, path=output_csv)

if __name__ == "__main__":
    write_nyc_inspection_to_csv(source=nyc_inspect_source,
                                id=nyc_inspect_data_id,
                                token=app_token,
                                output_csv=output_filename)
