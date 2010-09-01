
def assert_raises(excClass, func, *args, **kwargs):
    try: r = func(*args, **kwargs)
    except excClass: pass
    else: raise Exception('expected %s; got %s' % (excClass.__name__, r))
