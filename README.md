# ParallelRunManager

ParallelRunManager allows you to run multiple cmds/functions
    at the same time, you can even mix them while running!

usage:
    
    # new an instance with or without callback assigned:
    >>> runcmd_mgr = ParallelRunManager(callback)

    # add a cmd with or without specified callback (optional):
    >>> runcmd_mgr.add_cmd('docker ps', tag='ccloud')

    # add a fucntion with or without specified callback (optional):
    >>> runcmd_mgr.add_function(add, [3, 5], tag='function_add',
                                callback=callback_add)

    # execute them:
    >>> runcmd_mgr.execute()

The result will be stored in the given callback function
