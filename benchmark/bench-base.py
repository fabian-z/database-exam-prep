import datetime

# simple benchmark

# increase repetitions for accuracy
# decrease for benchmark speed
repetition = int(1e3)
start = datetime.datetime.now()

for i in range(0, repetition):
        #operation

end = datetime.datetime.now()

timing = (end - start) / repetition

print(timing)
#hour:minute:second:fractions of second
