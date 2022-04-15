def postprocess(model_outputs: list, *args, **kwargs) -> list:
    """
    input:
        model_outputs: a list containing DL model outputs.
    output: 
        A list containing the final scores of each DL model outputs.
    """
    scores = []
    return scores