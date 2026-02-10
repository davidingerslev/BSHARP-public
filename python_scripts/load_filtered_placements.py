from . import combined


# This exists so that older Quarto documents are backwards compatible
def get_dataframes(basepath: str):
    return combined.get_dataframes(basepath)
