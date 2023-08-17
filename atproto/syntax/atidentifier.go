package syntax

import (
	"fmt"
	"strings"
)

type AtIdentifier struct {
	Inner interface{}
}

func ParseAtIdentifier(raw string) (*AtIdentifier, error) {
	if strings.HasPrefix(raw, "did:") {
		did, err := ParseDID(raw)
		if err != nil {
			return nil, err
		}
		return &AtIdentifier{Inner: did}, nil
	}
	handle, err := ParseHandle(raw)
	if err != nil {
		return nil, err
	}
	return &AtIdentifier{Inner: handle}, nil
}

func (n AtIdentifier) AsHandle() (Handle, error) {
	handle, ok := n.Inner.(Handle)
	if ok {
		return handle, nil
	}
	return "", fmt.Errorf("AT Identifier is not a Handle")
}

func (n AtIdentifier) AsDID() (DID, error) {
	did, ok := n.Inner.(DID)
	if ok {
		return did, nil
	}
	return "", fmt.Errorf("AT Identifier is not a DID")
}

func (n AtIdentifier) Normalize() AtIdentifier {
	handle, ok := n.Inner.(Handle)
	if ok {
		return AtIdentifier{Inner: handle.Normalize()}
	}
	return n
}

func (n AtIdentifier) String() string {
	did, ok := n.Inner.(DID)
	if ok {
		return did.String()
	}
	handle, ok := n.Inner.(Handle)
	if ok {
		return handle.String()
	}
	return ""
}