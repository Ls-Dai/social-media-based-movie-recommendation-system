from deep_learning import infer
from analysis import postprocess


def sentiment_analysis(lines):
    model_output = infer(lines=lines)
    scores = postprocess(model_output=model_output)
    return scores
