// SPDX-License-Identifier: Apache-2.0
// Copyright 2019-2020 Authors of Cilium

package watchers

import (
	"context"
	"fmt"
	"time"

	selection2 "k8s.io/apimachinery/pkg/selection"

	"github.com/cilium/cilium/pkg/k8s"
	"github.com/cilium/cilium/pkg/k8s/informer"
	slim_corev1 "github.com/cilium/cilium/pkg/k8s/slim/k8s/api/core/v1"
	slim_networkingv1 "github.com/cilium/cilium/pkg/k8s/slim/k8s/api/networking/v1"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	intstr2 "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	IngressSubsys         = "IngressController"
	ciliumIngressPrefix   = "cilium_ingress_"
	ciliumIngressLabelKey = "cilium.io/ingress"
)

// TODO(michi) how do i get the namespace cilium is running?
const ciliumNamespace = "kube-system"

// event types
type ingressAddedEvent struct {
	ingress *slim_networkingv1.Ingress
}
type ingressUpdatedEvent struct {
	oldIngress *slim_networkingv1.Ingress
	newIngress *slim_networkingv1.Ingress
}

type ingressDeletedEvent struct {
	ingress *slim_networkingv1.Ingress
}

type serviceAddedEvent struct {
	service *slim_corev1.Service
}
type serviceUpdatedEvent struct {
	oldService *slim_corev1.Service
	newService *slim_corev1.Service
}
type serviceDeletedEvent struct {
	service *slim_corev1.Service
}

type ingressController struct {
	logger          logrus.FieldLogger
	ingressInformer cache.Controller
	ingressStore    cache.Store
	serviceInformer cache.Controller
	serviceStore    cache.Store
	queue           workqueue.RateLimitingInterface
	maxRetries      int
}

func (ic ingressController) handleAddService(obj interface{}) {
	service, ok := obj.(*slim_corev1.Service)
	if !ok {
		return
	}
	ic.queue.Add(serviceAddedEvent{service: service})
}

func (ic ingressController) handleUpdateService(oldObj, newObj interface{}) {
	oldService, ok := oldObj.(*slim_corev1.Service)
	if !ok {
		return
	}
	newService, ok := newObj.(*slim_corev1.Service)
	if !ok {
		return
	}
	ic.queue.Add(serviceUpdatedEvent{oldService: oldService, newService: newService})
}

func (ic ingressController) handleDeleteService(obj interface{}) {
	switch concreteObj := obj.(type) {
	case *slim_corev1.Service:
		ic.queue.Add(serviceDeletedEvent{service: concreteObj})
	case cache.DeletedFinalStateUnknown:
		service, ok := concreteObj.Obj.(*slim_corev1.Service)
		if ok {
			ic.queue.Add(serviceDeletedEvent{service: service})
		}
	default:
		return
	}
}

func NewIngressController(options ...IngressOption) (*ingressController, error) {
	opts := DefaultIngressOptions
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, fmt.Errorf("failed to apply option: %v", err)
		}
	}
	ic := ingressController{
		logger:     opts.Logger,
		queue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		maxRetries: opts.MaxRetries,
	}
	ic.ingressStore, ic.ingressInformer = informer.NewInformer(
		cache.NewListWatchFromClient(k8s.WatcherClient().NetworkingV1().RESTClient(), "ingresses", v1.NamespaceAll, fields.Everything()),
		&slim_networkingv1.Ingress{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ic.handleAddIngress,
			UpdateFunc: ic.handleUpdateIngress,
			DeleteFunc: ic.handleDeleteIngress,
		},
		nil,
	)

	labelSelector := labels.NewSelector()
	req, err := labels.NewRequirement(ciliumIngressLabelKey, selection2.Equals, []string{"true"})
	if err != nil {
		return nil, err
	}
	selectorFunc := func(options *v1meta.ListOptions) {
		labelSelector = labelSelector.Add(*req)
		options.LabelSelector = labelSelector.String()
	}
	ic.serviceStore, ic.serviceInformer = informer.NewInformer(
		cache.NewFilteredListWatchFromClient(k8s.WatcherClient().CoreV1().RESTClient(), "services", v1.NamespaceAll, selectorFunc),
		&slim_corev1.Service{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ic.handleAddService,
			UpdateFunc: ic.handleUpdateService,
			DeleteFunc: ic.handleDeleteService,
		},
		nil,
	)
	return &ic, nil
}

func (ic *ingressController) Run() {
	defer ic.queue.ShutDown()
	go ic.ingressInformer.Run(wait.NeverStop)
	if !cache.WaitForCacheSync(wait.NeverStop, ic.ingressInformer.HasSynced) {
		return
	}
	go ic.serviceInformer.Run(wait.NeverStop)
	if !cache.WaitForCacheSync(wait.NeverStop, ic.serviceInformer.HasSynced) {
		return
	}
	wait.Until(ic.controlLoop, time.Second, wait.NeverStop)
}

func (ic *ingressController) controlLoop() {
	for ic.processEvent() {
	}
}

func (ic *ingressController) processEvent() bool {
	event, shutdown := ic.queue.Get()
	if shutdown {
		return false
	}
	defer ic.queue.Done(event)
	err := ic.handleEvent(event)
	if err == nil {
		ic.queue.Forget(event)
	} else if ic.queue.NumRequeues(event) < ic.maxRetries {
		ic.queue.AddRateLimited(event)
	} else {
		ic.queue.Forget(event)
	}
	return true
}

func (ic *ingressController) handleIngressAddedEvent(event ingressAddedEvent) error {
	err := createLoadBalancer(event.ingress)
	if err != nil {
		ic.logger.WithError(err).WithField("ingress", event.ingress.Name).Warn("Failed to create a load balancer service for ingress")
	}
	return err
}

func (ic *ingressController) handleEvent(event interface{}) error {
	var err error
	switch ev := event.(type) {
	case ingressAddedEvent:
		ic.logger.Info("handling ingress added event")
		err = ic.handleIngressAddedEvent(ev)
		break
	case ingressUpdatedEvent:
		ic.logger.Info("handling ingress updated event")
		break
	case ingressDeletedEvent:
		ic.logger.Info("handling ingress deleted event")
		break
	case serviceAddedEvent:
		ic.logger.Info("handling service added event")
		break
	case serviceUpdatedEvent:
		ic.logger.Info("handling service updated event")
		break
	case serviceDeletedEvent:
		ic.logger.Info("handling service deleted event")
		break
	default:
		err = fmt.Errorf("received an unknown event: %s", ev)
	}
	return err
}

func getServiceNameForIngress(ingress *slim_networkingv1.Ingress) string {
	return ciliumIngressPrefix + ingress.Name
}

func getServiceForIngress(ingress *slim_networkingv1.Ingress) *v1.Service {
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service", // TODO(michi) find const
			APIVersion: "v1",      // TODO(michi) find const
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      getServiceNameForIngress(ingress),
			Namespace: ciliumNamespace,
			Labels:    map[string]string{ciliumIngressLabelKey: "true"},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "http",
					Protocol:   "TCP",
					Port:       80,
					TargetPort: intstr2.IntOrString{IntVal: 12345}, // TODO(michi) how to allocate a unique port
				},
				// TODO(michi) what about 443?
			},
			Selector: map[string]string{"k8s-app": "cilium"},
			Type:     v1.ServiceTypeLoadBalancer,
		},
	}
}

func createLoadBalancer(ingress *slim_networkingv1.Ingress) error {
	svc := getServiceForIngress(ingress)
	_, err := k8s.Client().CoreV1().Services(ciliumNamespace).Create(context.Background(), svc, metav1.CreateOptions{})
	if err != nil {
		log.WithError(err).WithField("ingress", ingress.Name).Error("Failed to create a service for ingress")
	}
	return err
}

func deleteLoadBalancer(ingress *slim_networkingv1.Ingress) error {
	err := k8s.Client().CoreV1().Services(ciliumNamespace).Delete(context.Background(), getServiceNameForIngress(ingress), metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).WithField("ingress", ingress.Name).Error("Failed to delete a service for ingress")
	}
	return nil
}

func createCiliumEnvoyConfig(ingress *slim_networkingv1.Ingress) error {
	// TODO(michi) implement
	return nil
}

func deleteCiliumEnvoyConfig(ingress *slim_networkingv1.Ingress) error {
	return nil
}

func (ic *ingressController) handleAddIngress(obj interface{}) {
	ingress, ok := obj.(*slim_networkingv1.Ingress)
	if !ok {
		return
	}
	ic.queue.Add(ingressAddedEvent{ingress: ingress})
}

func (ic *ingressController) handleUpdateIngress(oldObj, newObj interface{}) {
	oldIngress, ok := oldObj.(*slim_networkingv1.Ingress)
	if !ok {
		return
	}
	newIngress, ok := newObj.(*slim_networkingv1.Ingress)
	if !ok {
		return
	}
	ic.queue.Add(ingressUpdatedEvent{oldIngress: oldIngress, newIngress: newIngress})
}

func (ic *ingressController) handleDeleteIngress(obj interface{}) {
	switch concreteObj := obj.(type) {
	case *slim_networkingv1.Ingress:
		ic.queue.Add(ingressDeletedEvent{ingress: concreteObj})
	case cache.DeletedFinalStateUnknown:
		ingress, ok := concreteObj.Obj.(*slim_networkingv1.Ingress)
		if ok {
			ic.queue.Add(ingressDeletedEvent{ingress: ingress})
		}
	default:
		return
	}
}
