import matplotlib.pyplot as plt

def visualize(t,s,text):

    line, = plt.plot(t, s, lw=2)

    for i in xrange(len(t)) :
        plt.annotate(text[i], xy=(t[i], s[i]), xytext=(t[i], s[i]+0.25))

    plt.ylim(0,50)
    #plt.xlim(0,150)
    plt.show()


