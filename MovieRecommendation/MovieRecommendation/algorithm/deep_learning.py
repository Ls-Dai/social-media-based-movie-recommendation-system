def process(lines: dict) -> dict:
    """
    input:
        lines: A list of texts.
    output: 
        A list of DL model's predictions for each text input.
    """
    model_outputs = {
        '2000-01-01': {'pos': 0, 'neg': 0}, 
        '2000-01-02': {'pos': 0, 'neg': 0}, 
    }
    return model_outputs 