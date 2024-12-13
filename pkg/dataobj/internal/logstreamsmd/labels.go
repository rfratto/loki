package logstreamsmd

import "github.com/prometheus/prometheus/model/labels"

// Labels returns the Prometheus labels for the given identifier.
func Labels(ident *StreamIdentifier) labels.Labels {
	res := make(labels.Labels, 0, len(ident.Labels))
	for _, l := range ident.Labels {
		res = append(res, labels.Label{Name: l.Name, Value: l.Value})
	}
	return res
}

// Identifier returns the StreamIdentifier for the given labels.
func Identifier(lbls labels.Labels) *StreamIdentifier {
	res := make([]*StreamIdentifier_Label, 0, len(lbls))
	for _, l := range lbls {
		res = append(res, &StreamIdentifier_Label{Name: l.Name, Value: l.Value})
	}
	return &StreamIdentifier{Labels: res}
}
