import csv
import sodapy


def get_count(client, dataset):
    """
    Get number of rows
    """
    try:
        count = client.get(dataset, select='count(*)')
    except:
        'Request errored out.'
    return count[0]['count']

def list_to_csv(list, path):
    with open(path, "w") as output:
        writer = csv.writer(output, delimiter = ",")
        for item in list:
            writer.writerow(item)
