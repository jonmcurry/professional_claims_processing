from src.analysis.capacity import predict_throughput, predict_resource_usage


def test_predict_throughput():
    history = [10, 12, 14]
    assert predict_throughput(history) == 14


def test_predict_resource_usage():
    history = [50, 55, 60]
    assert predict_resource_usage(history) == 60
