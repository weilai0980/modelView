package basetool;

public class filterScan {

	public HBaseOp tabop = new HBaseOp();

	public filterScan() {

	}

	public void rowParser(String row, String inte[]) {
		int len = row.length();
		int st = 0, mid = 0;
		for (int i = 0; i < len; ++i) {
			if (row.charAt(i) == ',' && st == 0) {
				st = i + 1;
			} else if (row.charAt(i) == ',') {
				mid = i;
				break;
			}
		}
		inte[0] = row.substring(st, mid);
		inte[1] = row.substring(mid + 1, len);
		return;
	}

	public double pointQryTemp(double val, String tabname) {

		tabop.scanGloIni(tabname);
		tabop.scanMNext();
		String res = "";
		String[] lrbstr = new String[4];
		String str = "";
		double lbv = 0.0, rbv = 0.0;
		double num = 0.0;

		while ((str = tabop.scanR()) != "NO") {
			
			num++;

			rowParser(str, lrbstr);
			
			if (lrbstr[0].length() > 14 || lrbstr[1].length() > 14) {
				tabop.scanMNext();
				continue;
			}
			// rb = tabop.scanGet("attri", "ed");
			lbv = Double.parseDouble(lrbstr[0]);
			rbv = Double.parseDouble(lrbstr[1]);
			if (val >= lbv && val <= rbv) {

				res = tabop.scanGet("model", "cof1");

				// .......unit test...//

				// ...................//

				break;
			}
			tabop.scanMNext();
		}
		return num;
	}

	public void evalTemp(double model, double tl, double tr) {
		double step = (tr - tl) / 10.0;

		if (step > 0) {
			for (double i = tl; i <= tr; i += step) {

			}
		}
		return;
	}

	public double rangeQryTemp(double l, double r, String tabname) {

		tabop.scanGloIni(tabname);

		tabop.scanMNext();
		String model = "";
		double lbv = 0.0, rbv = 0.0;
		double cntnum = 0.0;
		String str = "";
		String[] lrbstr = new String[4];

		while ((str = tabop.scanR()) != "NO") {

			cntnum++;

			rowParser(str, lrbstr);
			// rb = tabop.scanGet("attri", "ed");
			lbv = Double.parseDouble(lrbstr[0]);
			rbv = Double.parseDouble(lrbstr[1]);
			if (lrbstr[0].length() > 14 || lrbstr[1].length() > 14) {
				tabop.scanMNext();
				continue;
			}

			if (lbv > r) {
				break;
			} else if (rbv < l) {

			} else {
				// .......unit test...//

				// ...................//
				model = tabop.scanGet("model", "cof1");
				// System.out.printf("%f   %f   %s\n",lbv,rbv,model);
				evalTemp(Double.parseDouble(model), lbv, rbv);
			}

			tabop.scanMNext();
		}
		return cntnum;
	}

	public double pointQryVal(double val, String tabname) {
		tabop.scanGloIni(tabname);

		double tp = 0.0;
		String lb = "", rb = "", model = "", st = "", ed = "";
		double lbv = 0.0, rbv = 0.0;
		double num = 0.0;

		while ((lb = tabop.scanNext("attri", "vl")) != "NO") {

			num++;

			rb = tabop.scanGet("attri", "vr");
			lbv = Double.parseDouble(lb);
			rbv = Double.parseDouble(rb);

			if (lb.length() > 14 || rb.length() > 14) {
				tabop.scanMNext();
				continue;
			}

			if (val >= lbv && val <= rbv) {
				// .......unit test...//

				// ...................//
				model = tabop.scanGet("model", "cof1");
				// st = tabop.scanGet("attri", "st");
				// ed = tabop.scanGet("attri", "ed");

				// evalTemp(Double.parseDouble(model),lbv, rbv);
			}

		}
		// System.out.printf("%d\n",num);
		return num;

	}

	public double rangeQryVal(double l, double r, String tabname) {

		tabop.scanGloIni(tabname);
		// tabop.scanMNext();
		double tp = 0.0;
		String lb = "", rb = "", model = "", st = "", ed = "";
		double lbv = 0.0, rbv = 0.0;
		double num = 0.0;

		while ((lb = tabop.scanNext("attri", "vl")) != "NO") {

			num++;
			rb = tabop.scanGet("attri", "vr");
			lbv = Double.parseDouble(lb);
			rbv = Double.parseDouble(rb);
			if (lbv > r || rbv < l) {

			} else {
				// .......unit test...//
				// num++;

				// ...................//
				model = tabop.scanGet("model", "cof1");
				evalTemp(Double.parseDouble(model), lbv, rbv);
				// st = tabop.scanGet("attri", "st");
				// ed = tabop.scanGet("attri", "ed");
			}

		}
		// System.out.printf("%d\n",num);
		return num;
	}

}
