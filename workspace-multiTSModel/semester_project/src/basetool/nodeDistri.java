package basetool;

import java.io.IOException;

public class nodeDistri {

	public double lowbound;

	public HBaseOp idxop = new HBaseOp();
	public double[] tree = new double[1000000];
	public double[] treecap = new double[1000000];
	public int treecnt = 0;
	public String tab;

	public nodeDistri(String tabname) throws IOException {

		idxop.creTab(tabname);
		idxop.iniTabOperation(tabname);
		tab = tabname;
		lowbound = 0.0;
	}

	public void bstCons() {

		idxop.scanGloIni(tab);
		String cur = "";
		double nodev = 0, nodecap = 0;
		int i = 0, len = 0;

		while ((cur = idxop.scanR()) != "NO") {

			i = 0;
			len = cur.length();
			for (i = 0; i < len; ++i) {
				if (cur.charAt(i) == ',') {
					break;
				}
			}
			nodev = Double.parseDouble(cur.substring(i + 1, len));
			nodecap = Double.parseDouble(idxop.scanGet("value", "count"));

			tree[treecnt] = nodev;
			treecap[treecnt] = nodecap + treecap[treecnt];
			treecnt++;
		}
		return;
	}

	public int bstNodeSch(double node, int sig) // -1: node not existent // sig:
												// -1 lower; 1 upper; 0 singel
												// node
	{
		int l = 0, r = treecnt - 1, mid = 0;
		while (l < r - 1) {
			mid = l + (r - l) / 2;
			if (tree[mid] > node) {
				r = mid;
			} else if (tree[mid] < node) {
				l = mid;
			} else if (Math.abs(tree[mid] - node) <= 1e-3) {
				return mid;
			}
		}
		if (Math.abs(tree[l] - node) <= 1e-3) {
			return l;
		} else if (Math.abs(tree[r] - node) <= 1e-3) {
			return r;
		} else {

			if (sig == 1) {
				return l;
			} else if (sig == -1) {
				return r;
			} else {
				return -1;
			}
		}
	}

	public double bstNodeCap(double node) {
		int idx = bstNodeSch(node, 0);
		if (idx == -1) {
			return 0;
		} else {
			return treecap[idx] - treecap[idx - 1];
		}
	}

	public double bstInterCap(double lnode, double rnode) {
		int lidx = bstNodeSch(lnode, -1), ridx = bstNodeSch(rnode, 1);
		return treecap[ridx] - treecap[lidx - 1];
	}

	public double bstRedunInterCap(double lnode, double rnode) {
		int lidx = bstNodeSch(lnode, -1), ridx = bstNodeSch(rnode, 1);
		return treecap[ridx] - treecap[lidx - 1]
				- (treecap[ridx] - treecap[ridx - 1])
				- (treecap[lidx] - treecap[lidx - 1]);
	}

	public double bstInterNodes(double lnode, double rnode) {
		int lidx = bstNodeSch(lnode, -1), ridx = bstNodeSch(rnode, 1);
		return ridx - lidx + 1;
	}

}
