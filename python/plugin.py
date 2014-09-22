import logging

plugins = dict()

class plugin(object):
  '''
  A simple plugins decorator.
  Decorator takes a name as input.
  Optionally, can be given one or more namespaces in addition to the name.
  Otherwise, will default to the 'default' namespace.

  Example:
    @plugin('test_func','one_namespace','two_namespace')
    def some_function():
      ...
    
    plugin.plugins
    {
      'default': {},
      'one_namespace': {'test_func': <function some_function at ...>},
      'two_namespace': {'test_func': <function some_function at ...>},
    }
  '''
  def __init__(self,name,*args):
    self.name = name
    self.args = args or ['default']

  def __call__(self,f):
    map(lambda x: plugins.setdefault(x,dict()).update({self.name:f}), self.args)
    return f
