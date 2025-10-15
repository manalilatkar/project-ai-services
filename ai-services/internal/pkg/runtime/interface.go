package runtime

type Runtime interface {
	ListImages() ([]string, error)
	ListPods() (any, error)
}
