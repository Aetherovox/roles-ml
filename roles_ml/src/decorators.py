import functools
import time


def timer(function):
    def wrapper(*args, **kwargs):
        t0 = time.time()
        x = function(*args, **kwargs)
        t1 = time.time()
        print("Process took {}".format(t1 - t0))
        return x

    return wrapper


# can time both functions and class methods
class Timer:
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.function = func
        self.t0 = time.time()
        self.duration = 0

    def __call__(self, *args, **kwargs):
        # run the function
        x = self.function(*args, **kwargs)
        t1 = time.time()
        self.duration = t1 - self.t0
        print("Process took {}".format(self.duration))
        return x
