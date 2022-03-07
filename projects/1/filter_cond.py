#
#
def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    if not line_dict['I1']:
        return False
    cond_match = (
       int(line_dict["I1"]) > 20
    ) 
    return True if cond_match else False
