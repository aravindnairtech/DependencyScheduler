from collections import defaultdict
import networkx as nx # Library for displaying graphs.
import matplotlib.pyplot as plt

class DependencyScheduler(object):

    def __init__(self):
        self.tasks = set()
        # The successors of a task are the tasks that depend on it, and can
        # only be done once the task is completed.
        self.successors = defaultdict(set)
        # The predecessors of a task have to be done before the task.
        self.predecessors = defaultdict(set)
        self.completed_tasks = set() # completed tasks

    def add_task(self, t, dependencies):
        """Adds a task t with given dependencies."""
        # Makes sure we know about all tasks mentioned.
        assert t not in self.tasks or len(self.predecessors[t]) == 0, "The task was already present."
        self.tasks.add(t)
        self.tasks.update(dependencies)
        # The predecessors are the tasks that need to be done before.
        self.predecessors[t] = set(dependencies)
        # The new task is a successor of its dependencies.
        for u in dependencies:
            self.successors[u].add(t)

    def reset(self):
        self.completed_tasks = set()

    @property
    def done(self):
        return self.completed_tasks == self.tasks


    def show(self):
        """We use the nx graph to display the graph."""
        g = nx.DiGraph()
        g.add_nodes_from(self.tasks)
        g.add_edges_from([(u, v) for u in self.tasks for v in self.successors[u]])
        node_colors = ''.join([('g' if v in self.completed_tasks else 'r')
                           for v in self.tasks])
        nx.draw(g, with_labels=True, node_color=node_colors)
        plt.show()

    @property
    def uncompleted(self):
        """Returns the tasks that have not been completed.
        This is a property, so you can say scheduler.uncompleted rather than
        scheduler.uncompleted()"""
        return self.tasks - self.completed_tasks

    def _check(self):
        """We check that if t is a successor of u, then u is a predecessor
        of t."""
        for u in self.tasks:
            for t in self.successors[u]:
                assert u in self.predecessors[t]
### Implementation of `available_tasks` and `mark_completed`.

def scheduler_available_tasks(self):
    """Returns the set of tasks that can be done in parallel.
    A task can be done if all its predecessors have been completed.
    And of course, we don't return any task that has already been
    completed."""
    # YOUR CODE HERE
    # if(set(self.successors) == set()):
    #     return set(self.predecessors)
    # else:
    #     setAvailableTasks = set()
    #     for i in self.successors:
    #         if(i not in self.predecessors):
    #             setAvailableTasks.add(i)
    setAvailableTasks = set()
    for i in self.tasks:
        if self.predecessors[i].issubset(self.completed_tasks):
            setAvailableTasks.add(i)

    for i in self.completed_tasks:
        if i in setAvailableTasks:
            setAvailableTasks.remove(i)
            
    return setAvailableTasks
    

        # setAvailableTasks = set()
        # for i in self.successors:
        #     for j in self.predecessors:
        #         # if j in setAvailableTasks:
        #         #     break
                

        #         if (i != j):
        #             setAvailableTasks.add(j)
                

    
        

    return setAvailableTasks

    



def scheduler_mark_completed(self, t):
    """Marks the task t as completed, and returns the additional
    set of tasks that can be done (and that could not be
    previously done) once t is completed."""
    # YOUR CODE HERE
    self.completed_tasks.add(t)
    ans = set()
    
    for i in self.successors[t]:
        if self.predecessors[i].issubset(self.completed_tasks):
            ans.add(i)
            # self.successors[i].remove(t)
            # self.predecessors[i].remove(t)
            # self.tasks.remove(t)    
    
    # self.successors.remove(t)
    # self.tasks.remove(t)
    # self.predecessors.remove(t)

   

    return ans


DependencyScheduler.available_tasks = property(scheduler_available_tasks)
DependencyScheduler.mark_completed = scheduler_mark_completed

# Here is a place where you can test your code. 
import random
for _ in range(10000):
    s=DependencyScheduler()
    max=10
    tasks=[a for a in range(max)]
    dependentDict={}
    l=random.randint(0,max)
    for i in range(l):
        task=tasks.pop(tasks.index(random.choice(tasks)))
        d=[str(a) for a in random.sample(tasks, random.randint(0, len(tasks)))]
        dependentDict[str(task)]=d
        for a in d:
            dependentDict[a]=[] if a not in dependentDict else dependentDict[a]
        s.add_task(str(task), d)
    while len(dependentDict)>0:
        testedAvail=s.available_tasks
        trueAvail={a for a in dependentDict if dependentDict[a]==None or len(dependentDict[a])==0}
        try:
            assert testedAvail==trueAvail
        except:
            print('task/depend',dependentDict)
            print('tested available',testedAvail)
            print('actual available',trueAvail)
            s.show()
            assert testedAvail==trueAvail
        toDo = random.choice(list(trueAvail))
        MarkCompleted=s.mark_completed(toDo)
        dependentDict.pop(toDo)
        for a in dependentDict:
            if toDo in dependentDict[a]:
                dependentDict[a].remove(toDo)
        newTrueAvail={a for a in dependentDict if dependentDict[a]==None or len(dependentDict[a])==0}
        newTestAvail=s.available_tasks
        try:
            assert newTestAvail==newTrueAvail
            assert (MarkCompleted==set() or MarkCompleted.issubset(newTrueAvail)) and (MarkCompleted==set() or not MarkCompleted.issubset(trueAvail))
        except:
            print('task/depend',dependentDict)
            print('tested available',newTestAvail)
            print('actual available',newTrueAvail)
            print('your mark_completed of',toDo,':',MarkCompleted)
            s.show()
            assert newTestAvail==newTrueAvail
            assert (MarkCompleted==set() or MarkCompleted.issubset(newTrueAvail)) and (MarkCompleted==set() or not MarkCompleted.issubset(trueAvail))



import random

def execute_schedule(s, show=False):
    s.reset()
    in_process = s.available_tasks
    print("Starting by doing:", in_process)
    while len(in_process) > 0:
        # Picks one random task to be the first to be completed.
        t = random.choice(list(in_process))
        print("Completed:", t)
        in_process = in_process - {t} | s.mark_completed(t)
        print("Now doing:", in_process)
        if show:
            s.show()
    # Have we done all?
    if not s.done:
        print("Error, there are tasks that could not be completed:", s.uncompleted)

 #Pasta Tests :D

carbonara = DependencyScheduler()

# First, the part about cooking the pancetta.
carbonara.add_task('dice onions', [])
carbonara.add_task('dice pancetta', [])
carbonara.add_task('put oil and butter in pan', [])
carbonara.add_task('put pancetta in pan', ['dice pancetta'])
carbonara.add_task('put onions in pan', ['dice onions'])
carbonara.add_task('cook pancetta', ['put oil and butter in pan',
                                     'put pancetta in pan',
                                     'put onions in pan'])

# Second, the part about beating the eggs.
carbonara.add_task('put eggs in bowl', [])
carbonara.add_task('beat eggs', ['put eggs in bowl'])

# Third, cooking the pasta.
carbonara.add_task('fill pot with water', [])
carbonara.add_task('bring pot of water to a boil', ['fill pot with water'])
carbonara.add_task('add salt to water', ['bring pot of water to a boil'])
carbonara.add_task('put pasta in water', ['bring pot of water to a boil',
                                         'add salt to water'])
carbonara.add_task('colander pasta', ['put pasta in water'])

# And finally, we can put everything together.
carbonara.add_task('serve', ['beat eggs', 'cook pancetta', 'colander pasta'])

# Let's look at our schedule!
carbonara.show()


def execute_schedule(s, show=False):
    s.reset()
    in_process = s.available_tasks
    print("Starting by doing:", in_process)
    while len(in_process) > 0:
        # Picks one random task to be the first to be completed.
        t = random.choice(list(in_process))
        print("Completed:", t)
        in_process = in_process - {t} | s.mark_completed(t)
        print("Now doing:", in_process)
        if show:
            s.show()
    # Have we done all?
    if not s.done:
        print("Error, there are tasks that could not be completed:", s.uncompleted)

        
class RunSchedule(object):

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.in_process = None # Indicating, we don't know yet.

    def reset(self):
        self.scheduler.reset()
        self.in_process = None

    def step(self):
        """Performs a step, returning the task, if any, or None,
        if there is no step that can be done."""
        # If we don't know what steps are in process, we get them.
        if self.in_process is None:
            self.in_process = self.scheduler.available_tasks
        if len(self.in_process) == 0:
            return None
        t = random.choice(list(self.in_process))
        self.in_process = self.in_process - {t} | self.scheduler.mark_completed(t)
        return t

    @property
    def done(self):
        return self.scheduler.done

    def run(self):
        """Runs the scheduler from the current configuration to completion.
        You must call reset() first, if you want to run the whole schedule."""
        tasks = []
        while not self.done:
            t = self.step()
            if t is not None:
                tasks.append(t)
        return tasks
def dependency_scheduler_redo(self, t):
    """Mark the task t, and all its successors, as undone.
    Returns the set of successor tasks of t, with t included."""
    # YOUR CODE HERE
    #

    returnsetoft = set()
    vopen = {t}
    vclosed = set()
    while len(vopen) > 0:
        u = vopen.pop()
        vclosed.add(u)
        vopen.update(self.successors[u] - vclosed)
    for i in vclosed:
        if i in self.completed_tasks:
            returnsetoft.add(i)
            self.completed_tasks.remove(i)
    returnsetoft.add(t)
    #self.completed_tasks.remove(t)
    return returnsetoft
        # return vclosed
    # returnsetoft = set()
    # self.completed_tasks.remove(t)  # removes t as well from completed_tasks set (marks as undone)
    # for i in self.successors[t]:
    #     returnsetoft.add(i)
    #     if (i in self.completed_tasks):
    #         self.completed_tasks.remove(i)  # removes successors of t from completed_tasks list (marks as undone)
    #
    # returnsetoft.add(t)

    #return returnsetoft
DependencyScheduler.redo = dependency_scheduler_redo



# Here is a place where you can test your code. 
def reachable_from_hw_10(g, v):
    """Given a graph g, and a starting vertex v, returns the set of states
    reachable from v in g."""
    vopen = {v}
    vclosed = set()
    while len(vopen) > 0:
        u = vopen.pop()
        vclosed.add(u)
        vopen.update(g.successors[u] - vclosed)
    return vclosed

import random
for _ in range(10000):
    s=DependencyScheduler()
    sCopy=DependencyScheduler()
    max=4
    tasks=[a for a in range(max)]
    dependentDict={}
    l=random.randint(1,max)
    for i in range(l):
        task=tasks.pop(tasks.index(random.choice(tasks)))
        d=[str(a) for a in random.sample(tasks, random.randint(0, len(tasks)))]
        dependentDict[str(task)]=d
        for a in d:
            dependentDict[a]=[] if a not in dependentDict else dependentDict[a]
        s.add_task(str(task),d)
        sCopy.add_task(str(task),d)
    doNum = random.randint(1,l)
    done = []
    while len(dependentDict)>=doNum:
        toDo = random.choice(list(s.available_tasks))
        done.append(toDo)
        s.mark_completed(toDo)
        sCopy.mark_completed(toDo)
        dependentDict.pop(toDo)
        for a in dependentDict:
            if toDo in dependentDict[a]:
                dependentDict[a].remove(toDo)
    uncomp = s.uncompleted
    toRedo=done.pop(done.index(random.choice(done)))
    sredo=s.redo(toRedo)
    ans=reachable_from_hw_10(s,toRedo)
    for a in ans:
      if a in uncomp:
        ans=ans-{a}
    try:
        assert sredo|uncomp==s.uncompleted or sredo == {None}
        assert ans==sredo
    except:
        print('old:')
        sCopy.show()
        print('tried to redo',toRedo)
        print('should return',ans)
        print(' you returned',sredo)
        print(s.uncompleted,'!=',sredo,'+',uncomp,'=',sredo|uncomp)
        s.show()
        assert sredo|uncomp==s.uncompleted or sredo == {None}
        assert ans==sredo

        
### Tests for `redo` for code. 5 points. 

def assert_equal(a, b):
    assert a == b

s = DependencyScheduler()
s.add_task('a', [])
s.add_task('b', ['a'])
s.add_task('c', ['a'])
s.add_task('d', ['b', 'c'])
s.add_task('e', ['a', 'd'])

s.mark_completed('a')
s.mark_completed('b')
s.mark_completed('c')
assert_equal(s.available_tasks, {'d'})
s.redo('b')
assert_equal(s.available_tasks, {'b'})

# Additional test
s = DependencyScheduler()
s.add_task('a', [])
s.add_task('b', ['a'])
s.add_task('c', ['a'])
s.add_task('d', ['b', 'c'])
s.add_task('e', ['a', 'd'])

s.mark_completed('a')
s.mark_completed('b')
s.mark_completed('c')
s.mark_completed('d')
s.redo('a')
assert_equal(s.available_tasks, {'a'})

s = DependencyScheduler()
s.add_task('a', [])
s.add_task('b', ['a'])
s.add_task('c', ['a'])
s.add_task('d', ['b', 'c'])
s.add_task('e', ['a', 'd'])
s.mark_completed('a')
s.mark_completed('b')
s.mark_completed('c')
assert_equal(s.available_tasks, {'d'})
s.mark_completed('d')
s.mark_completed('e')
s.redo('e')
assert_equal(s.available_tasks, {'e'})


def run_schedule_redo(self, t):
    """Marks t as to be redone."""
    # We drop everything that was in progress.
    # This also forces us to ask the scheduler for what to redo.
    self.in_process = None
    return self.scheduler.redo(t)

RunSchedule.redo = run_schedule_redo

runner = RunSchedule(carbonara)
runner.reset()
for _ in range(10):
    print(runner.step())
print("---> readd salt")
print("marking undone:", runner.redo("add salt to water"))
print("completed:", runner.scheduler.completed_tasks)
for _ in range(10):
    print(runner.step())
print("--->redo dice pancetta")
print("marking undone:", runner.redo("dice pancetta"))
print("completed:", runner.scheduler.completed_tasks)
for t in runner.run():
    print(t)

    
### Implementation of `cooking_redo`


def dependency_scheduler_cooking_redo(self, v):
    """Indicates that the task v needs to be redone, as something went bad.
    This is the "cooking" version of the redo, in which the redo propagates
    to both successors (as for code) and predecessors."""
    
    self.redo(v)
    redoPredecessors = self.predecessors[v]
    redoPredecessors.add(v)
    self.completed_tasks -= redoPredecessors
    
    

    # returnsetoft = set()
    # vopen = {t}
    # vclosed = set()
    # while len(vopen) > 0:
    #     u = vopen.pop()
    #     vclosed.add(u)
    #     vopen.update(self.successors[u] - vclosed)
    # for i in vclosed:
    #     if i in self.completed_tasks:
    #         returnsetoft.add(i)
    #         self.completed_tasks.remove(i)
    # returnsetoft.add(t)
    # #self.completed_tasks.remove(t)
   




DependencyScheduler.cooking_redo = dependency_scheduler_cooking_redo




### `AND_OR_Scheduler` implementation

class AND_OR_Scheduler(object):

    def __init__(self):
        self.tasks = set()
        # The successors of a task are the tasks that depend on it, and can
        # only be done once the task is completed.
        self.successors = defaultdict(set)
        # The predecessors of a task have to be done before the task.
        self.predecessors = defaultdict(set)
        self.completed_tasks = set()  # completed tasks
        self.andtasks = set()
        self.ortasks = set()

    # It is up to you to implement the initialization.
    # YOUR CODE HERE
    def add_task(self, t, dependencies):
        """Adds a task t with given dependencies."""
        # Makes sure we know about all tasks mentioned.
        assert t not in self.tasks or len(self.predecessors[t]) == 0, "The task was already present."
        self.tasks.add(t)
        self.tasks.update(dependencies)
        # The predecessors are the tasks that need to be done before.
        self.predecessors[t] = set(dependencies)
        # The new task is a successor of its dependencies.
        for u in dependencies:
            self.successors[u].add(t)

    def add_and_task(self, t, dependencies):
        """Adds an AND task t with given dependencies."""
        self.add_task(t,dependencies)
        self.andtasks.add(t)
        # YOUR CODE HERE

    def add_or_task(self, t, dependencies):
        """Adds an OR task t with given dependencies."""
        self.add_task(t,dependencies)
        self.ortasks.add(t)
        # YOUR CODE HERE

    @property
    def done(self):
        return self.completed_tasks == self.tasks
    # YOUR CODE HERE

    @property
    def available_tasks(self):
        """Returns the set of tasks that can be done in parallel.
        A task can be done if:
        - It is an AND task, and all its predecessors have been completed, or
        - It is an OR task, and at least one of its predecessors has been completed.
        And of course, we don't return any task that has already been
        completed."""
        setAvailableTasks = set()
        for i in self.tasks:
            if set([i]).issubset(self.andtasks):
                if self.predecessors[i].issubset(self.completed_tasks):
                    setAvailableTasks.add(i)
            elif set([i]).issubset(self.ortasks):
                for j in self.predecessors[i]:
                    if set([j]).issubset(self.completed_tasks):
                        setAvailableTasks.add(i)
                        setAvailableTasks.add(j)
            else:
                if self.predecessors[i].issubset(self.completed_tasks):
                    setAvailableTasks.add(i)

        for i in self.completed_tasks:
            if i in setAvailableTasks:
                setAvailableTasks.remove(i)

        return setAvailableTasks
        # YOUR CODE HERE

    def mark_completed(self, t):
        """Marks the task t as completed, and returns the additional
        set of tasks that can be done (and that could not be
        previously done) once t is completed."""
        self.completed_tasks.add(t)
        ans = set()
        for i in self.successors[t]:
            if set(i).issubset(self.andtasks):
                if self.predecessors[i].issubset(self.completed_tasks):
                    ans.add(i)
            elif set(i).issubset(self.ortasks):
                if not self.predecessors[i].issubset(self.completed_tasks):
                    ans.add(i)
        return ans
        # YOUR CODE HERE

    def show(self):
        """You can use the nx graph to display the graph.  You may want to ensure
        that you display AND and OR nodes differently."""
        # YOUR CODE HERE
        """We use the nx graph to display the graph."""
        g = nx.DiGraph()
        g.add_nodes_from(self.tasks)
        g.add_edges_from([(u, v) for u in self.tasks for v in self.successors[u]])
        node_colors = ''.join([('g' if v in self.completed_tasks else 'r')
                               for v in self.tasks])
        nx.draw(g, with_labels=True, node_color=node_colors)
        plt.show()
        
        
