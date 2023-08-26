def merge_safe(*dicts: dict) -> dict:
    """
    Merge dictionaries raising a ValueError if there's a duplicate key
    """

    merged_dict = {}

    for dictionary in dicts:
        for key, value in dictionary.items():
            if key in merged_dict:
                raise ValueError("Duplicate key: " + key)

            merged_dict[key] = value

    return merged_dict
