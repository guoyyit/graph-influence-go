package influence

import "math"

// tCritical returns the two-sided t critical value for a confidence
// level in (0,1) with df degrees of freedom, via inversion of the
// regularized incomplete beta function I_{df/(df+t^2)}(df/2, 1/2).
func tCritical(confidence float64, df int) float64 {
	if df <= 0 {
		return 0
	}
	p := (1 - confidence) / 2 // upper-tail probability
	if df >= 1000 {
		return invNormalCDF(1-p) * 1 // normal approximation for large df
	}
	// Find t with upper-tail probability p using bisection.
	lo, hi := 0.0, 10.0
	for studentTUpperTail(hi, df) > p {
		hi *= 2
	}
	for i := 0; i < 80; i++ {
		mid := (lo + hi) / 2
		if studentTUpperTail(mid, df) > p {
			lo = mid
		} else {
			hi = mid
		}
	}
	return (lo + hi) / 2
}

// studentTUpperTail returns P(T > t) for T ~ t(df).
func studentTUpperTail(t float64, df int) float64 {
	if t == 0 {
		return 0.5
	}
	x := float64(df) / (float64(df) + t*t)
	a := float64(df) / 2
	b := 0.5
	ib := regularizedIncompleteBeta(x, a, b)
	if t > 0 {
		return 0.5 * ib
	}
	return 1 - 0.5*ib
}

// regularizedIncompleteBeta computes I_x(a,b) by continued fraction
// (Numerical Recipes betacf), with direct evaluation near the edges.
func regularizedIncompleteBeta(x, a, b float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	bt := math.Exp(lgamma(a+b) - lgamma(a) - lgamma(b) + a*math.Log(x) + b*math.Log(1-x))
	if x < (a+1)/(a+b+2) {
		return bt * betaCF(x, a, b) / a
	}
	return 1 - bt*betaCF(1-x, b, a)/b
}

func betaCF(x, a, b float64) float64 {
	const maxIter = 200
	const eps = 3e-14
	const tiny = 1e-30
	qab := a + b
	qap := a + 1
	qam := a - 1
	c := 1.0
	d := 1 - qab*x/qap
	if math.Abs(d) < tiny {
		d = tiny
	}
	d = 1 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		m2 := 2 * m
		aa := float64(m) * (b - float64(m)) * x / ((qam + float64(m2)) * (a + float64(m2)))
		d = 1 + aa*d
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		h *= d * c
		aa = -(a + float64(m)) * (qab + float64(m)) * x / ((a + float64(m2)) * (qap + float64(m2)))
		d = 1 + aa*d
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = 1 + aa/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		del := d * c
		h *= del
		if math.Abs(del-1) < eps {
			break
		}
	}
	return h
}

// lgamma is math.Lgamma returning only the value.
func lgamma(x float64) float64 {
	v, _ := math.Lgamma(x)
	return v
}

// invNormalCDF is Acklam's rational approximation to the standard
// normal quantile function.
func invNormalCDF(p float64) float64 {
	const (
		a1 = -3.969683028665376e+01
		a2 = 2.209460984245205e+02
		a3 = -2.759285104469687e+02
		a4 = 1.383577518672690e+02
		a5 = -3.066479806614716e+01
		a6 = 2.506628277459239e+00
		b1 = -5.447609879822406e+01
		b2 = 1.615858368580409e+02
		b3 = -1.556989798598866e+02
		b4 = 6.680131188771972e+01
		b5 = -1.328068155288572e+01
		c1 = -7.784894002430293e-03
		c2 = -3.223964580411365e-01
		c3 = -2.400758277161838e+00
		c4 = -2.549732539343734e+00
		c5 = 4.374664141464968e+00
		c6 = 2.938163982698783e+00
		d1 = 7.784695709041462e-03
		d2 = 3.224671290700398e-01
		d3 = 2.445134137142996e+00
		d4 = 3.754408661907416e+00
	)
	plow := 0.02425
	phigh := 1 - plow
	var q, r float64
	switch {
	case p < plow:
		q = math.Sqrt(-2 * math.Log(p))
		return (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	case p <= phigh:
		q = p - 0.5
		r = q * q
		return (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r + a6) * q /
			(((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r + 1)
	default:
		q = math.Sqrt(-2 * math.Log(1-p))
		return -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	}
}
