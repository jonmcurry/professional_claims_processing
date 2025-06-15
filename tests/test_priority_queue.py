from src.utils.priority_queue import PriorityClaimQueue


def test_priority_queue_order():
    q = PriorityClaimQueue()
    q.push({"id": 1}, 1)
    q.push({"id": 2}, 5)
    q.push({"id": 3}, 3)
    order = [q.pop()["id"] for _ in range(3)]
    assert order == [2, 3, 1]
