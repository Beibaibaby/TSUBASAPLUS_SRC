import numpy as np
import matplotlib.pyplot as plt
import math

r1=np.load('./result1.npy')
r2=np.load('./result2.npy')
r3=np.load('./result3.npy')
r4=np.load('./result4.npy')
r5=np.load('./result5.npy')
r6=np.load('./result6.npy')
r7=np.load('./result7.npy')
r8=np.load('./result8.npy')
total_rs = np.concatenate((r1, r2, r3, r4, r5, r6, r7, r8))
print(total_rs)
print(np.mean(total_rs))
print(np.std(total_rs))

# plt.hist(r1, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r2, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r3, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r4, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r5, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r6, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r7, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.hist(r8, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
plt.hist(total_rs, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
plt.title("Pair Matrix Histogram")
plt.xlabel("Jumping Steps")
plt.ylabel("Count")
plt.show()

# ax1 = plt.subplot(241)
# ax1.plot(r1, bin)
# ax2 = plt.subplot(242)
# ax2.plot(t,s,color="y",linestyle = "-")
# ax3 = plt.subplot(243)
# ax3.plot(t,s,color="g",linestyle = "-.")
# ax4 = plt.subplot(244)
# ax4.plot(t,s,color="b",linestyle = ":")
# ax5 = plt.subplot(245)
# ax5.plot(t,s, color="r",linestyle = "--")
# ax6 = plt.subplot(246)
# ax6.plot(t,s,color="y",linestyle = "-")
# ax7 = plt.subplot(247)
# ax7.plot(t,s,color="g",linestyle = "-.")
# ax8 = plt.subplot(248)
# ax8.plot(t,s,color="b",linestyle = ":")
# plt.hist(, bins = [0,5,10,15,20,25,30,35,40,45,50,55,60])
# plt.title("Pair Matrix Histogram")
# plt.xlabel("Jumping Steps")
# plt.ylabel("Count")
# plt.show()