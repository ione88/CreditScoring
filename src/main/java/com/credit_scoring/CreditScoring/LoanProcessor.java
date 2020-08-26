package main.java.com.credit_scoring.CreditScoring;

public class LoanProcessor {
	
	private ScoreCalculator scoreCalculator;
	
	public LoanProcessor(int id) {
		this.scoreCalculator = new ScoreCalculator(id);
	}
	
	public boolean isLoanApproved(double annual_inc, int loan_amnt, String purpose) {
		double debt_to_income_ratio = loan_amnt/annual_inc;
		int score = scoreCalculator.calcFinalScore(annual_inc, debt_to_income_ratio, purpose);
		if (score >= 576) return true;
		else return false;
	}
	
	public int getLoanScore(double annual_inc, int loan_amnt, String purpose) {
		double debt_to_income_ratio = loan_amnt/annual_inc;
		return scoreCalculator.calcFinalScore(annual_inc, debt_to_income_ratio, purpose);
	}
	
	public static void main(String[] args) {
		LoanProcessor lp = new LoanProcessor(54734);
		int score = lp.getLoanScore(30000, 6000, "credit_card");
		System.out.println(score);
	}
}
