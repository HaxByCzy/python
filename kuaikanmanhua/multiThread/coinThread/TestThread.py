import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import time, threading

num = 0
lock = threading.Lock()
class MyThread(threading.Thread):
    def __init__(self, threadName ,counter):
        threading.Thread.__init__(self)
        self.counter = counter
        self.threadName = threadName

    def run(self):
        for i in range(1, 3):
            self.counter = self.counter + 10
            print  self.threadName, self.counter
        self.changeGobal(self.counter)

    def changeGobal(self, counter):
        global num
        lock.acquire()
        try:
            num = num + counter
            print "---- ", num, counter
        finally:
            lock.release()

def out():
    mt1 = MyThread("thread1", 0)
    mt2 = MyThread("thread2", 0)
    mt3 = MyThread("thread3", 0)
    #mt4 = MyThread("thread4", 0)
    mt1.start()
    mt2.start()
    mt3.start()
   # mt4.start()
    mt1.join()
    mt2.join()
    mt3.join()
   # mt4.join()
    print "end ", num

def test():
    out()


if __name__ == "__main__":
    test()