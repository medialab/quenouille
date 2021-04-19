import time
from quenouille.utils import SmartTimer

timer = SmartTimer(7, print, ('drrrrring'))
timer.start()

for i in range(5):
    time.sleep(1)
    print(timer.elapsed(), timer.remaining())
