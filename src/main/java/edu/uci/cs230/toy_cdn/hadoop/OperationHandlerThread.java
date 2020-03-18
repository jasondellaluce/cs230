package edu.uci.cs230.toy_cdn.hadoop;

public class OperationHandlerThread extends Thread {

	private int period;
	private MapReduceOperation mapReduceOperation;
	private ResultVisitor resultVisitor;

	public OperationHandlerThread(int period, MapReduceOperation mapReduceOperation,
			ResultVisitor resultVisitor) {
		this.period = period;
		this.mapReduceOperation = mapReduceOperation;
		this.resultVisitor = resultVisitor;
	}

	@Override
	public void run() {
		while(true) {
			try {
				Thread.sleep(period * 1000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
			
			try {
				if(!mapReduceOperation.run()) {
					System.err.println("ERROR in MapReduce: " + mapReduceOperation.getClass().getSimpleName());
					return;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				return;
			};
			
			System.out.println("Operation completed, starting visitation...");
			try {
				mapReduceOperation.acceptResultVisitor(resultVisitor);
			}
			catch (Exception e) {
				e.printStackTrace();
				return;
			}
			System.out.println("Visitation completed!");
			
		}
	}

}
