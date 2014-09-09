package basetool;

import basetool.segm;

import java.io.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;

import java.util.*;

import org.eclipse.jdt.internal.core.util.Util;

public class segbTree {

	public String[][] nd = new String[10000][6000];
	// public ArrayList<Integer>[] tree = new ArrayList[10];

	public int[][] tree = new int[10000][6000];
	public int[] treenum = new int[10000];
	public int root = 0, knum = 0;

	public int base = 20;

	public segbTree(int M) {
		root = 0;
		knum = 0;

		nd = new String[10000][6000];
		tree = new int[10000][6000];
		treenum = new int[10000];

		base = M;
		// for(int i=0;i<10;i++)
		// {
		// tree[i]=new ArrayList<String>;
		// }

	}

	public void ini(int M) {
		root = 0;
		knum = 0;

		base = M;
		// for(int i=0;i<10;i++)
		// {
		// tree[i]=new ArrayList<String>;
		// }

	}

	// public void crtTTree(){
	public void crtTTree(String[] smal, String[] big, int num) {
		int j = 0, k = 0;

		String[] tmp = new String[num];
		for (int i = 0; i < num; i++) {
			tmp[i] = smal[i] + "," + big[i];
		}

		java.util.Arrays.sort(tmp);

		for (int i = 0; i < num; ++i) {
			nd[j][k] = tmp[i];
			treenum[j] = k++;
			if (k == base) {
				j++;
				k = 0;
			}
		}

		// if (num % 100 != 0)
		if (k != 0)
			j++;

		knum = j - 1;

		int rn = 10, st = 0, cnt = j;

		while (rn > 1) {

			k = 0;
			for (int i = st; i < j; ++i) {

				nd[cnt][k] = nd[i][treenum[i]];// modificatoin

				tree[cnt][k] = i;
				treenum[cnt] = k++;

				if (k == base) {
					cnt++;
					k = 0;
				}
			}

			if (k != 0)
				cnt++;

			rn = cnt - j;
			st = j;
			j = cnt;
		}

		root = cnt - 1;

		// System.out.printf("  %d\n", cnt-1);

		return;
	}

	public void crtVTree(String[] tbeg, String[] tend, double[] vsm,
			double[] vbig, int num) {
		int j = 0, k = 0;

		String[] tmp = new String[num];
		for (int i = 0; i < num; i++) {
			tmp[i] = Double.toString(vsm[i]) + "," + Double.toString(vbig[i])
					+ "," + tbeg[i] + "," + tend[i];
		}

		java.util.Arrays.sort(tmp);

		for (int i = 0; i < num; ++i) {
			nd[j][k] = tmp[i];// Double.toString(vsm[i]) + "," +
								// Double.toString(vbig[i])+","+tbeg[i]+","+tend[i];
			treenum[j] = k++;

			if (k == base) {
				j++;
				k = 0;
			}
		}

		if (k != 0)
			j++;

		knum = j - 1;

		int rn = 10, st = 0, cnt = j;

		while (rn > 1) {

			k = 0;
			for (int i = st; i < j; ++i) {

				nd[cnt][k] = nd[i][treenum[i]];

				tree[cnt][k] = i;
				treenum[cnt] = k++;

				if (k == base) {
					cnt++;
					k = 0;
				}
			}

			if (k != 0)
				cnt++;

			rn = cnt - j;
			st = j;
			j = cnt;
		}

		root = cnt - 1;

		return;
	}

	public void viEt(String str, String[] res) {
		int st = 0;

		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == ',') {
				if (st != 0) {
					res[0] = str.substring(0, st);
					res[1] = str.substring(st + 1, i);
					break;
				}
				st = i;
			}
		}
		return;
	}

	public void schVpTree(String vp, ArrayList<String> res)// res: index of
															// node and key
	{
		int[] rsmal = new int[6];
		int[] rbig = new int[6];
		int[] tmp = new int[6];

		schVpTreePro(vp, rsmal, rbig);
		String[] vint = new String[6];

		if (rsmal[0] > rbig[0]) {
			tmp = rsmal;
			rsmal = rbig;
			rbig = tmp;
		}

		if (rsmal[0] == rbig[0]) {
			for (int i = rsmal[1]; i <= rbig[1]; i++) {
				viEt(nd[rsmal[0]][i], vint);

				if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
					res.add(nd[rsmal[0]][i]);
			}
			return;
		}

		for (int i = rsmal[1]; i < base; i++) {

			viEt(nd[rsmal[0]][i], vint);

			if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
				res.add(nd[rsmal[0]][i]);
		}

		for (int i = rsmal[0] + 1; i < rbig[0]; ++i) {
			for (int j = 0; j < base; ++j) {

				viEt(nd[i][j], vint);

				if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
					res.add(nd[i][j]);
			}
		}
		for (int i = 0; i <= rbig[1]; i++) {

			viEt(nd[rbig[0]][i], vint);

			if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
				res.add(nd[rbig[0]][i]);
		}
		return;

	}

	public void schVpTreeLowBd(String vp, ArrayList<String> res)// res: index of
	// node and key
	{
		int[] rsmal = new int[6];
		int[] rbig = new int[6];
		int[] tmp = new int[6];

		schVpTreeProLowBd(vp, rsmal, rbig);
		String[] vint = new String[6];

	    rsmal[0]=0;
	    rsmal[1]=0;
		
		if (rsmal[0] > rbig[0]) {
			tmp = rsmal;
			rsmal = rbig;
			rbig = tmp;
		}

		if (rsmal[0] == rbig[0]) {
			for (int i = rsmal[1]; i <= rbig[1]; i++) {
				viEt(nd[rsmal[0]][i], vint);

				if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
					res.add(nd[rsmal[0]][i]);
			}
			return;
		}

		for (int i = rsmal[1]; i < treenum[rsmal[0]]; i++) {

			viEt(nd[rsmal[0]][i], vint);

			if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
				res.add(nd[rsmal[0]][i]);
		}

		for (int i = rsmal[0] + 1; i < rbig[0]; ++i) {
			for (int j = 0; j < treenum[i]; ++j) {

				viEt(nd[i][j], vint);

				if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
					res.add(nd[i][j]);
			}
		}
		for (int i = 0; i <= rbig[1]; i++) {

			viEt(nd[rbig[0]][i], vint);

			if (vp.compareTo(vint[0]) >= 0 && vp.compareTo(vint[1]) <= 0)
				res.add(nd[rbig[0]][i]);
		}
		return;

	}

	public void schVpTreeProLowBd(String vp, int[] rsmal, int[] rbig) {
		String str = "";
		int cur = root;
		String smal = "", big = "";
		int i = 0;

		// cur=root;
		// while (true) {// up direction
		// for (i = treenum[cur]; i >= 0; i--) {
		// int st = 0;
		//
		// //..........key extraction...........//
		// for (int j = 0; j < nd[cur][i].length(); j++) {
		//
		// if (nd[cur][i].charAt(j) == ',') {
		// if (st != 0) {
		// smal = nd[cur][i].substring(0, st);
		// big = nd[cur][i].substring(st + 1, j);
		// break;
		// }
		// st = j;
		// }
		// }
		// //...................................//
		//
		// if (vp.compareTo(big) > 0) {
		// break;
		// }
		// }
		//
		// ++i;
		//
		// if(i>treenum[cur])
		// i--;
		//
		// if (cur <= knum) {// && vp.compareTo(smal) >= 0) {
		// break;
		// }
		// cur = tree[cur][i];
		// }
		//
		// if (i > treenum[cur]) {
		// cur += 1;
		// rsmal[0] = cur;
		// rsmal[1] = 0;
		// } else {
		// rsmal[0] = cur;
		// rsmal[1] = i;
		// }
		cur = root;
		String presm = "", prebig = "";
		while (true) {// down direction
			for (i = 0; i <= treenum[cur]; i++) {
				int st = 0;
				
				//............value interval extraction......//
				for (int j = 0; j < nd[cur][i].length(); j++) {

					if (nd[cur][i].charAt(j) == ',') {
						if (st != 0) {
							smal = nd[cur][i].substring(0, st);
							big = nd[cur][i].substring(st + 1, j);
							break;
						}
						st = j;
					}
				}
                //..........................................//
				
				// if(vp.compareTo(smal)>0)
				// {
				// continue;
				// }
				// else
				// break;
				if (vp.compareTo(smal) <= 0) {
					break;
				}

				presm = smal;
				prebig = big;
			}

			//if (vp.compareTo(prebig) <= 0)
				//i--;

			if (i < 0)
				i = 0;

			if (i > treenum[cur])
				i = treenum[cur];

			if (cur <= knum) {
				break;
			}
			cur = tree[cur][i];
		}

		rsmal[0] = 0;
		rsmal[1] = 0;
		
		rbig[0] = cur;
		rbig[1] = i;
	}

	public void schVpTreePro(String vp, int[] rsmal, int[] rbig)// res: index
																// of node
																// and key
	{
		String str = "";
		int cur = root;
		String smal = "", big = "";
		int i = 0;

		cur = root;
		while (true) {// up direction
			for (i = treenum[cur]; i >= 0; i--) {
				int st = 0;

				// ..........key extraction...........//
				for (int j = 0; j < nd[cur][i].length(); j++) {

					if (nd[cur][i].charAt(j) == ',') {
						if (st != 0) {
							smal = nd[cur][i].substring(0, st);
							big = nd[cur][i].substring(st + 1, j);
							break;
						}
						st = j;
					}
				}
				// ...................................//

				if (vp.compareTo(big) > 0) {
					break;
				}
			}

			++i;

			if (i > treenum[cur])
				i--;

			if (cur <= knum) {// && vp.compareTo(smal) >= 0) {
				break;
			}
			cur = tree[cur][i];
		}

		if (i > treenum[cur]) {
			cur += 1;
			rsmal[0] = cur;
			rsmal[1] = 0;
		} else {
			rsmal[0] = cur;
			rsmal[1] = i;
		}
		cur = root;
		String presm = "", prebig = "";
		while (true) {// down direction
			for (i = 0; i <= treenum[cur]; i++) {
				int st = 0;
				for (int j = 0; j < nd[cur][i].length(); j++) {

					if (nd[cur][i].charAt(j) == ',') {
						if (st != 0) {
							smal = nd[cur][i].substring(0, st);
							big = nd[cur][i].substring(st + 1, j);
							break;
						}
						st = j;
					}
				}

				// if(vp.compareTo(smal)>0)
				// {
				// continue;
				// }
				// else
				// break;
				if (vp.compareTo(smal) <= 0) {
					break;
				}

				presm = smal;
				prebig = big;
			}

			if (vp.compareTo(prebig) <= 0)
				i--;

			if (i < 0)
				i = 0;

			if (i > treenum[cur])
				i = treenum[cur];

			if (cur <= knum) {
				break;
			}
			cur = tree[cur][i];
		}

		rbig[0] = cur;
		rbig[1] = i;

		// return nd[cur][i];
	}

	public String schTpTree(String tp, int[] res)// res: index of node and key
	{
		String str = "";
		int cur = root;
		String smal = "", big = "";
		int i = 0;
		while (true) {
			for (i = 0; i <= treenum[cur]; ++i) {
				for (int j = 0; j < nd[cur][i].length(); j++) {
					if (nd[cur][i].charAt(j) == ',') {
						smal = nd[cur][i].substring(0, j);
						big = nd[cur][i].substring(j + 1, nd[cur][i].length());
						break;
					}
				}
				if (tp.compareTo(big) <= 0) {
					break;
				}
			}
			if (cur <= knum && tp.compareTo(smal) >= 0) {
				break;
			}
			cur = tree[cur][i];
		}

		res[0] = cur;
		res[1] = i;

		return nd[cur][i];
	}

	public void scanTIntTree(String tsmal, String tbig, ArrayList<String> res) {

		int[] st = new int[4];
		int[] ed = new int[4];

		String lb = "", upb = "";
		lb = schTpTree(tsmal, st);
		upb = schTpTree(tbig, ed);

		// ...text.........//
		// System.out.printf("%s\n%s\n",lb,upb);
		// ................//

		if (st[0] == ed[0]) {
			for (int i = st[1]; i <= ed[1]; i++) {
				res.add(nd[st[0]][i]);
			}
			return;
		}

		for (int i = st[1]; i < base; i++) {
			res.add(nd[st[0]][i]);
		}

		for (int i = st[0] + 1; i < ed[0]; ++i) {
			for (int j = 0; i < base; ++j) {
				res.add(nd[i][j]);
			}
		}
		for (int i = 0; i <= ed[1]; i++) {
			res.add(nd[ed[0]][i]);
		}
		return;
	}

	

	public int maxval(int a, int b) {
		if (a > b)
			return a;
		else
			return b;
	}

	public int minval(int a, int b) {
		if (a > b)
			return b;
		else
			return a;
	}

	public void scanVIntTree(String vsmal, String vbig, ArrayList<String> res) {

		int[] st1 = new int[6];
		int[] ed1 = new int[6];
		int[] st2 = new int[6];
		int[] ed2 = new int[6];

		int[] st = new int[6];
		int[] ed = new int[6];

		schVpTreePro(vbig, st2, ed2);
		schVpTreePro(vsmal, st1, ed1);

		if (st1[0] < st2[0]) {
			st[0] = st1[0];
			st[1] = st1[1];
		} else {
			if (st1[0] == st2[0]) {
				st[0] = st2[0];

				st[1] = minval(st1[1], st2[1]);
			} else {
				st[0] = st2[0];
				st[1] = st2[1];
			}
		}

		if (ed1[0] > ed2[0]) {
			ed[0] = ed1[0];
			ed[1] = ed1[1];
		} else {

			if (ed1[0] == ed2[0]) {
				ed[0] = ed2[0];

				ed[1] = maxval(ed1[1], ed2[1]);
			} else {
				ed[0] = ed2[0];
				ed[1] = ed2[1];
			}
		}

		String[] vint = new String[6];

		if (st[0] == ed[0]) {
			for (int i = st[1]; i <= ed[1]; i++) {
				viEt(nd[st[0]][i], vint);

				if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

				} else
					res.add(nd[st[0]][i]);
			}
			return;
		}
		for (int i = st[1]; i < base; i++) {

			viEt(nd[st[0]][i], vint);

			if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

			} else
				res.add(nd[st[0]][i]);
		}

		for (int i = st[0] + 1; i < ed[0]; ++i) {
			for (int j = 0; j < base; ++j) {

				viEt(nd[i][j], vint);
				if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

				} else
					res.add(nd[i][j]);
			}
		}
		for (int i = 0; i <= ed[1]; i++) {

			viEt(nd[ed[0]][i], vint);

			if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

			} else
				res.add(nd[ed[0]][i]);
		}
		return;

	}
	
	public void scanVIntTreeLowBd(String vsmal, String vbig, ArrayList<String> res) {

		int[] st1 = {0,0};
		int[] ed1 = {0,0};
		
		int[] st2 = new int[6];
		int[] ed2 = new int[6];

		int[] st = new int[6];
		int[] ed = new int[6];

		schVpTreeProLowBd(vbig, st2, ed2);
		schVpTreeProLowBd(vsmal, st1, ed1);

//		if (st1[0] < st2[0]) {
//			st[0] = st1[0];
//			st[1] = st1[1];
//		} else {
//			if (st1[0] == st2[0]) {
//				st[0] = st2[0];
//
//				st[1] = minval(st1[1], st2[1]);
//			} else {
//				st[0] = st2[0];
//				st[1] = st2[1];
//			}
//		}
		
		st[0]=0;
		st[1]=0;
		
		

		if (ed1[0] > ed2[0]) {
			ed[0] = ed1[0];
			ed[1] = ed1[1];
		} else {

			if (ed1[0] == ed2[0]) {
				ed[0] = ed2[0];

				ed[1] = maxval(ed1[1], ed2[1]);
			} else {
				ed[0] = ed2[0];
				ed[1] = ed2[1];
			}
		}

		String[] vint = new String[6];

		if (st[0] == ed[0]) {
			for (int i = st[1]; i <= ed[1]; i++) {
				viEt(nd[st[0]][i], vint);

				if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

				} else
					res.add(nd[st[0]][i]);
			}
			return;
		}
		for (int i = st[1]; i < treenum[0]; i++) {

			viEt(nd[st[0]][i], vint);

			if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

			} else
				res.add(nd[st[0]][i]);
		}

		for (int i = st[0] + 1; i < ed[0]; ++i) {
			for (int j = 0; j < treenum[i]; ++j) {

				viEt(nd[i][j], vint);
				if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

				} else
					res.add(nd[i][j]);
			}
		}
		for (int i = 0; i <= ed[1]; i++) {

			viEt(nd[ed[0]][i], vint);

			if (vsmal.compareTo(vint[1]) > 0 || vbig.compareTo(vint[0]) < 0) {

			} else
				res.add(nd[ed[0]][i]);
		}
		return;

	}

}
	
