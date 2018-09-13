import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


public class Test {

	
	public static void main(String[] args) {
		List<BigDecimal> bigDecimals = new ArrayList<BigDecimal>();;
		bigDecimals.stream().forEach(s ->{
			s.add(BigDecimal.ZERO);
		});
	}
}
