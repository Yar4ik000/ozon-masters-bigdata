#
#
def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    if line_dict["I1"] == '':
        return False
    cond_match = (
       20 < int(line_dict["I1"]) < 40 
    ) 
    return True if cond_match else False
