"""
Utility functions specific to Yelp data
"""

def to_str(items):
    """
    Convert elements of list to string
    """
    output = []
    for item in items:
        output.append(str(item))
    return output


def convert_to_yelp_phone_numbers(phone_numbers):
    """
    Input list of phone numbers
    Adds 1 to phone numbers to match Yelp phone standard
    """
    phone_numbers = to_str(phone_numbers)
    my_list = []
    for phone_number in phone_numbers:
        if len(phone_number) == 10:
            my_list.append('1' + phone_number)
        else:
            my_list.append(phone_number)
    return my_list
