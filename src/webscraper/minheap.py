import math

class MinHeap:
    @staticmethod
    def parent_index(i):
        # Return index of node parent for node at index i
        if i == 0:
            return None
        return (i - 1) // 2
    
    @staticmethod
    def left_child_index(i):
        return 2 * i + 1

    @staticmethod
    def right_child_index(i):
        return 2 * i + 2

    def __init__(self, max_capacity=2000):
        # Max Capacity = Maximum number of links in queue at one time

        self.links = [None] * max_capacity
        self.last = 0
        self.indices = {}
        self.visited = []

    def size(self):
        # Number of links within the queue at a given time  minheap.py

        return self.last
    
    def is_empty(self):

        return self.last == 0
    
    def next_up(self):
        if self.is_empty():
            return None
        return self.links[0]
    
    def _swap(self, a, b):
        # Swap entries at indices a and b
        assert (a < self.last) and (b < self.last)
        
        # Unpack pre-swap links for reindexing
        _, a_link = self.links[a]
        _, b_link = self.links[b]

        # Swap
        temp = self.links[a]
        self.links[a] = self.links[b]
        self.links[b] = temp

        # Reindex
        self.indices[a_link] = b
        self.indices[b_link] = a

    def _sift_up(self, position):
        """
        Recursively move inserted node into correct position
        """
        # Priority of child node
        current_priority, _ = self.links[position]

        if position == 0: # move to top of queue
            return 
        # Priority of parent node
        parent_index = MinHeap.parent_index(position)
        parent_priority, _ = self.links[parent_index]
        
        if current_priority < parent_priority:
            self._swap(position, parent_index)
            self._sift_up(parent_index) # Continue to move node up
        else: 
            return # order is correct, end recursion

    def _sift_down(self, position):
        """
        Recursively re-sort after a node is removed
        """
        current_priority, _ = self.links[position]
        left_child_index = MinHeap.left_child_index(position)
        right_child_index = MinHeap.right_child_index(position)

        if left_child_index >= self.last:
            return # No left child, no sifting required
        
        left_child_priority, _ = self.links[left_child_index]
        right_child_priority, _ = self.links[right_child_index]

        if (left_child_index < self.last) and (right_child_index >= self.last):
            # Node has only left child - swap if necessary
            minimum = min(current_priority, left_child_priority)
            if left_child_priority == minimum:
                self._swap(position, left_child_index)
                self._sift_down(left_child_index)
        else:
            # Node has left and right children - swap the lower of the two if necessary
            minimum = min([current_priority, left_child_priority, right_child_priority])
            if left_child_priority == minimum:
                self._swap(position, left_child_index)
                self._sift_down(left_child_index)
            elif right_child_priority == minimum:
                self._swap(position, right_child_index)
                self._sift_down(right_child_index)
    
    def remove_next(self):
        """
        Remove the next up link and return it
        """
        if self.is_empty():
            return
        
        next_priority, next_link = self.next_up()
        if self.last > 0: 
            # More than 1 link, sorting required
            # Replace removed item with last item in heap and sort accordingly
            last = self.links[self.last - 1]
            _, last_link = last
            self.links[0] = last
            self.indices[last_link] = 0
            self.last -= 1

            self._sift_down(0)
            del self.indices[next_link]
        
        self.visited.append(next_link)
        return (next_priority, next_link)
    
    def insert(self, priority, link):
        """
        Add link to queue
        """
        if link in list(self.indices.keys()):
            print("copy detected")
        self.links[self.last] = (priority, link)
        self.indices[link] = self.last
        self.last += 1
        if self.last > 1:
            self._sift_up(self.last - 1)
    


    ### TESTING METHODS ### 

    def _verify(self, position):
        """
        Verifies that node is correctly placed by searching below it,
        starting at node given by index = position
        """
        if position >= self.last:
            return True
        current = self.links[position]
        _, current_link = self.links[position]
        if self.indices[current_link] != position:
            return False
        left_child_index = MinHeap.left_child_index(position)
        right_child_index = MinHeap.right_child_index(position)
        if right_child_index < self.last:
            return (
                current <= self.links[right_child_index]
                and current <= self.links[right_child_index]
                and self._verify(right_child_index)
                and self._verify(right_child_index)
            )
        if left_child_index < self.last:
            return curr <= self.data[left_child_index] and self._verify(left_child_index)
        return True


    def verify(self):
        """
        Verifies that the entire tree is correctly sorted by calling _verify()
        on the first index
        """

        return self._verify(0)

    def show(self):
        if self.is_empty():
            print("[empty]")
            return
        linebreak = 1
        row_count = 0
        for i in range(self.last):
            print(self.links[i], end = " ")
            row_count += 1
            if (row_count == linebreak) or (i == self.last - 1):
                print()
                linebreak *= 2
                row_count = 0
        