// todo

- ROC: roll-over counter, a high-order counter to keep the monotonicity of the RTP sequence number,
because RTP sequence numbers are only 16 bits, when they reach the max value they wrap to 0 and the ROC
allows to count that roll-over