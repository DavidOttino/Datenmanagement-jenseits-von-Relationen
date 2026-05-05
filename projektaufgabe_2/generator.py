import random

def generate(l: int, sparsity: float):

    if not (0.0 <= sparsity <= 1.0):
        raise ValueError("sparsity has to be between 0 and 1")

    m = l - 1
    n = l - 1

    # A: m x l
    # B: l x n

    A = [
        [random_value(sparsity) for _ in range(l)]
        for _ in range(m)
    ]

    B = [
        [random_value(sparsity) for _ in range(n)]
        for _ in range(l)
    ]

    return A, B

def random_value(sparsity):
    if random.random() < sparsity:
        return 0.0
    else:
        return random.uniform(-10, 10)