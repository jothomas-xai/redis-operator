package k8sutils

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	common "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	retry "github.com/avast/retry-go"
	redis "github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// RedisDetails will hold the information for Redis Pod
type RedisDetails struct {
	PodName   string
	Namespace string
}

func (rd *RedisDetails) FQDN() string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", rd.PodName, common.GetHeadlessServiceNameFromPodName(rd.PodName), rd.Namespace, envs.GetServiceDNSDomain())
}

func (rd *RedisDetails) String() string {
	return fmt.Sprintf("%s.%s", rd.PodName, rd.Namespace)
}

// getRedisServerIP will return the IP of redis service
func getRedisServerIP(ctx context.Context, client kubernetes.Interface, redisInfo RedisDetails) string {
	log.FromContext(ctx).V(1).Info("Fetching Redis pod", "namespace", redisInfo.Namespace, "podName", redisInfo.PodName)

	redisPod, err := client.CoreV1().Pods(redisInfo.Namespace).Get(context.TODO(), redisInfo.PodName, metav1.GetOptions{})
	if err != nil {
		log.FromContext(ctx).Error(err, "Error in getting Redis pod IP", "namespace", redisInfo.Namespace, "podName", redisInfo.PodName)
		return ""
	}

	redisIP := redisPod.Status.PodIP
	log.FromContext(ctx).V(1).Info("Fetched Redis pod IP", "ip", redisIP)

	// Check if IP is empty
	if redisIP == "" {
		log.FromContext(ctx).V(1).Info("Redis pod IP is empty", "namespace", redisInfo.Namespace, "podName", redisInfo.PodName)
		return ""
	}

	// If we're NOT IPv4, assume we're IPv6..
	if net.ParseIP(redisIP).To4() == nil {
		log.FromContext(ctx).V(1).Info("Redis is using IPv6", "ip", redisIP)
	}

	log.FromContext(ctx).V(1).Info("Successfully got the IP for Redis", "ip", redisIP)
	return redisIP
}

func getRedisServerAddress(ctx context.Context, client kubernetes.Interface, rd RedisDetails, port int) string {
	return formatRedisAddress(getRedisServerIP(ctx, client, rd), port)
}

func getEndpoint(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, rd RedisDetails) string {
	var (
		host string
		port int
	)
	port = *cr.Spec.Port
	if cr.Spec.ClusterVersion != nil && *cr.Spec.ClusterVersion == "v7" {
		host = rd.FQDN()
	} else {
		host = getRedisServerIP(ctx, client, rd)
		if host == "" {
			return ""
		}
	}
	if cr.Spec.KubernetesConfig.GetServiceType() == "NodePort" {
		svc, err := getService(ctx, client, cr.Namespace, rd.PodName)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to get service for redis pod", "Pod", rd.PodName)
			return ""
		}
		if svc.Spec.Type != corev1.ServiceTypeNodePort {
			log.FromContext(ctx).Error(errors.New("service type mismatch"), "Expected NodePort service type", "Pod", rd.PodName, "ActualType", svc.Spec.Type)
			return ""
		}
		svcPort, ok := lo.Find(svc.Spec.Ports, func(item corev1.ServicePort) bool {
			return item.Name == "redis-client"
		})
		if ok {
			port = int(svcPort.NodePort)
		}
		pod, err := client.CoreV1().Pods(rd.Namespace).Get(ctx, rd.PodName, metav1.GetOptions{})
		if err != nil {
			log.FromContext(ctx).Error(err, "")
			return ""
		}
		host = pod.Status.HostIP
	}
	return host + ":" + strconv.Itoa(port)
}

// CreateSingleLeaderRedisCommand will create command for single leader cluster creation
func CreateSingleLeaderRedisCommand(ctx context.Context, cr *rcvb2.RedisCluster) RedisInvocation {
	cmd := RedisInvocation{
		Command:      []string{"redis-cli"},
		RedisCommand: []string{"CLUSTER", "ADDSLOTS"},
	}
	for i := 0; i < 16384; i++ {
		cmd.RedisCommand = append(cmd.RedisCommand, strconv.Itoa(i))
	}
	log.FromContext(ctx).V(1).Info("Generating Redis Add Slots command for single node cluster",
		"BaseCommand", []string{"redis-cli", "CLUSTER", "ADDSLOTS"},
		"SlotsRange", "0-16383",
		"TotalSlots", 16384)

	return cmd
}

// RepairDisconnectedMasters attempts to repair disconnected/failed masters by issuing
// a CLUSTER MEET with the updated address of the host
func RepairDisconnectedMasters(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()
	return repairDisconnectedMasters(ctx, client, cr, redisClient)
}

func repairDisconnectedMasters(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, redisClient *redis.Client) error {
	nodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		return err
	}
	masterNodeType := "master"
	var lastError error
	for _, node := range nodes {
		if !nodeIsOfType(node, masterNodeType) {
			continue
		}
		if !nodeFailedOrDisconnected(node) {
			continue
		}
		host, err := getMasterHostFromClusterNode(node)
		if err != nil {
			lastError = err
			log.FromContext(ctx).V(1).Error(err, "Failed to get pod name from cluster node. Continuing with other nodes.", "Node", node)
			continue
		}
		ip := getRedisServerIP(ctx, client, RedisDetails{
			// host may be FQDN like redis-cluster-leader-0.redis-cluster-leader-headless.default.svc.cluster.local
			// or it may be like redis-cluster-leader-0
			// we need to adapt
			PodName:   strings.Split(host, ".")[0],
			Namespace: cr.Namespace,
		})
		err = redisClient.ClusterMeet(ctx, ip, strconv.Itoa(*cr.Spec.Port)).Err()
		if err != nil {
			lastError = err
			log.FromContext(ctx).V(1).Error(err, "Failed to execute CLUSTER MEET on node. Continuing with other nodes.", "Node", node)
			continue
		}
	}
	return lastError
}

func getMasterHostFromClusterNode(node clusterNodesResponse) (string, error) {
	addressAndHost := node[1]
	s := strings.Split(addressAndHost, ",")
	if len(s) != 2 {
		return "", fmt.Errorf("failed to extract host from host and address string, unexpected number of elements: %d", len(s))
	}
	return strings.Split(addressAndHost, ",")[1], nil
}

// CreateMultipleLeaderRedisCommand will create command for single leader cluster creation
func CreateMultipleLeaderRedisCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) RedisInvocation {
	cmd := RedisInvocation{
		Command: []string{"redis-cli", "--cluster", "create"},
	}
	replicas := cr.Spec.GetReplicaCounts("leader")
	for podCount := 0; podCount < int(replicas); podCount++ {
		rd := RedisDetails{
			PodName:   cr.Name + "-leader-" + strconv.Itoa(podCount),
			Namespace: cr.Namespace,
		}
		cmd.AddFlag(getEndpoint(ctx, client, cr, rd))
	}
	cmd.AddFlag("--cluster-yes")
	return cmd
}

// RedisInvocation models an invocation of redis-cli
type RedisInvocation struct {
	Command      []string // e.g. {"redis-cli", "--cluster", "create"}
	Flags        []string // e.g. {"-h", "localhost", "-p", "6379"}
	RedisCommand []string // e.g. {"CLUSTER", "ADDSLOTS", "1", "2", "3"}
}

// Builds the full argv for executeCommand
func (ri *RedisInvocation) Args() []string {
	args := append([]string{}, ri.Command...)
	args = append(args, ri.Flags...)
	args = append(args, ri.RedisCommand...)
	return args
}

func (ri *RedisInvocation) AddFlag(flag ...string) *RedisInvocation {
	ri.Flags = append(ri.Flags, flag...)
	return ri
}

// ExecuteRedisClusterCommand will execute redis cluster creation command
func ExecuteRedisClusterCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) {
	var cmd RedisInvocation
	replicas := cr.Spec.GetReplicaCounts("leader")
	switch int(replicas) {
	case 1:
		err := executeFailoverCommand(ctx, client, cr, "leader")
		if err != nil {
			log.FromContext(ctx).Error(err, "error executing failover command")
		}
		cmd = CreateSingleLeaderRedisCommand(ctx, cr)
	default:
		cmd = CreateMultipleLeaderRedisCommand(ctx, client, cr)
	}

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(ctx, client, cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			log.FromContext(ctx).Error(err, "Error in getting redis password")
		}
		cmd.AddFlag("-a")
		cmd.AddFlag(pass)
	}
	cmd.AddFlag(getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)
	executeCommand(ctx, client, cr, cmd.Args(), cr.Name+"-leader-0")
}

func getRedisTLSArgs(tlsConfig *commonapi.TLSConfig, clientHost string) []string {
	cmd := []string{}
	if tlsConfig != nil {
		cmd = append(cmd, "--tls")
		cmd = append(cmd, "--cacert")
		cmd = append(cmd, "/tls/ca.crt")
		cmd = append(cmd, "--insecure")
	}
	return cmd
}

// createRedisReplicationCommand will create redis replication creation command
func createRedisReplicationCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, leaderPod RedisDetails, followerPod RedisDetails) []string {
	cmd := []string{"redis-cli", "--cluster", "add-node"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, followerPod))
	cmd = append(cmd, getEndpoint(ctx, client, cr, leaderPod))
	cmd = append(cmd, "--cluster-slave")
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(ctx, client, cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to retrieve Redis password", "Secret", *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name)
		} else {
			cmd = append(cmd, "-a", pass)
		}
	}
	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, leaderPod.PodName)...)
	return cmd
}

// ExecuteRedisReplicationCommand will execute the replication command
func ExecuteRedisReplicationCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) {
	var podIP string
	followerCounts := cr.Spec.GetReplicaCounts("follower")
	leaderCounts := cr.Spec.GetReplicaCounts("leader")
	followerPerLeader := followerCounts / leaderCounts

	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()

	nodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to get cluster nodes")
	}
	for followerIdx := 0; followerIdx <= int(followerCounts)-1; {
		for i := 0; i < int(followerPerLeader) && followerIdx <= int(followerCounts)-1; i++ {
			followerPod := RedisDetails{
				PodName:   cr.Name + "-follower-" + strconv.Itoa(followerIdx),
				Namespace: cr.Namespace,
			}
			leaderPod := RedisDetails{
				PodName:   cr.Name + "-leader-" + strconv.Itoa((followerIdx)%int(leaderCounts)),
				Namespace: cr.Namespace,
			}
			podIP = getRedisServerIP(ctx, client, followerPod)
			if !checkRedisNodePresence(ctx, nodes, podIP) {
				log.FromContext(ctx).V(1).Info("Adding node to cluster.", "Node.IP", podIP, "Follower.Pod", followerPod)
				cmd := createRedisReplicationCommand(ctx, client, cr, leaderPod, followerPod)
				redisClient := configureRedisClient(ctx, client, cr, followerPod.PodName)
				pong, err := redisClient.Ping(ctx).Result()
				redisClient.Close()
				if err != nil {
					log.FromContext(ctx).Error(err, "Failed to ping Redis server", "Follower.Pod", followerPod)
					continue
				}
				if pong == "PONG" {
					executeCommand(ctx, client, cr, cmd, cr.Name+"-leader-0")
				} else {
					log.FromContext(ctx).V(1).Info("Skipping execution of command due to failed Redis ping", "Follower.Pod", followerPod)
				}
			} else {
				log.FromContext(ctx).V(1).Info("Skipping Adding node to cluster, already present.", "Follower.Pod", followerPod)
			}

			followerIdx++
		}
	}
}

type clusterNodesResponse []string

// clusterNodes will returns the response of CLUSTER NODES
func clusterNodes(ctx context.Context, redisClient *redis.Client) ([]clusterNodesResponse, error) {
	output, err := redisClient.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, err
	}

	csvOutput := csv.NewReader(strings.NewReader(output))
	csvOutput.Comma = ' '
	csvOutput.FieldsPerRecord = -1
	csvOutputRecords, err := csvOutput.ReadAll()
	if err != nil {
		return nil, err
	}
	response := make([]clusterNodesResponse, 0, len(csvOutputRecords))
	for _, record := range csvOutputRecords {
		response = append(response, record)
	}
	return response, nil
}

// ExecuteFailoverOperation will execute redis failover operations
func ExecuteFailoverOperation(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	err := executeFailoverCommand(ctx, client, cr, "leader")
	if err != nil {
		return err
	}
	err = executeFailoverCommand(ctx, client, cr, "follower")
	if err != nil {
		return err
	}
	return nil
}

// executeFailoverCommand will execute failover command
func executeFailoverCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, role string) error {
	replicas := cr.Spec.GetReplicaCounts(role)
	podName := fmt.Sprintf("%s-%s-", cr.Name, role)
	for podCount := 0; podCount <= int(replicas)-1; podCount++ {
		log.FromContext(ctx).V(1).Info("Executing redis failover operations", "Redis Node", podName+strconv.Itoa(podCount))
		client := configureRedisClient(ctx, client, cr, podName+strconv.Itoa(podCount))
		defer client.Close()
		cmd := redis.NewStringCmd(ctx, "cluster", "reset")
		err := client.Process(ctx, cmd)
		if err != nil {
			log.FromContext(ctx).Error(err, "Redis command failed with this error")
			flushcommand := redis.NewStringCmd(ctx, "flushall")
			err = client.Process(ctx, flushcommand)
			if err != nil {
				log.FromContext(ctx).Error(err, "Redis flush command failed with this error")
				return err
			}
		}
		err = client.Process(ctx, cmd)
		if err != nil {
			log.FromContext(ctx).Error(err, "Redis command failed with this error")
			return err
		}
		output, err := cmd.Result()
		if err != nil {
			log.FromContext(ctx).Error(err, "Redis command failed with this error")
			return err
		}
		log.FromContext(ctx).V(1).Info("Redis cluster failover executed", "Output", output)
	}
	return nil
}

// CheckRedisNodeCount will check the count of redis nodes, excluding any stale
// nodes the cluster has flagged as failed.
func CheckRedisNodeCount(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, nodeType string) int32 {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()
	clusterNodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to get cluster nodes")
	}
	count := countClusterNodes(clusterNodes, nodeType)
	if nodeType != "" {
		log.FromContext(ctx).V(1).Info("Number of redis nodes are", "Nodes", strconv.Itoa(int(count)), "Type", nodeType)
	} else {
		log.FromContext(ctx).V(1).Info("Total number of redis nodes are", "Nodes", strconv.Itoa(int(count)))
	}
	return count
}

// countClusterNodes counts cluster nodes of the requested type ("leader",
// "follower", or "" for every node), skipping any node the cluster has flagged
// as failed. Stale "master,fail" ghosts - left behind when a pod loses its
// identity after a restart - would otherwise be counted as live members,
// inflating the leader and total node counts. That inflation traps
// reconciliation in an early-return loop and stops the operator from ever
// reaching its relabel/repair logic.
func countClusterNodes(nodes []clusterNodesResponse, nodeType string) int32 {
	var redisNodeType string
	switch nodeType {
	case "leader":
		redisNodeType = "master"
	case "follower":
		redisNodeType = "slave"
	default:
		redisNodeType = nodeType
	}

	var count int32
	for _, node := range nodes {
		if nodeIsFailed(node) {
			continue
		}
		if redisNodeType == "" || nodeIsOfType(node, redisNodeType) {
			count++
		}
	}
	return count
}

// RedisClusterStatusHealth use `redis-cli --cluster check 127.0.0.1:6379`
func RedisClusterStatusHealth(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) bool {
	logger := log.FromContext(ctx)
	leaderReplicas := cr.Spec.GetReplicaCounts("leader")

	// Try to check cluster health from multiple leader nodes with retry logic
	var lastErr error
	for i := int32(0); i < leaderReplicas; i++ {
		podName := fmt.Sprintf("%s-leader-%d", cr.Name, i)

		// Retry logic with exponential backoff for each node
		err := retry.Do(
			func() error {
				return checkClusterHealth(ctx, client, cr, podName)
			},
			retry.Attempts(3),
			retry.Delay(500*time.Millisecond),
			retry.DelayType(retry.BackOffDelay),
			retry.OnRetry(func(n uint, err error) {
				logger.V(1).Info("Retrying cluster health check", "pod", podName, "attempt", n+1, "error", err)
			}),
		)

		if err == nil {
			// Successfully verified cluster health from this node
			logger.V(1).Info("Cluster health check passed", "pod", podName)
			return true
		}

		lastErr = err
		logger.V(1).Info("Cluster health check failed from node", "pod", podName, "error", err)
	}

	// All nodes failed the health check
	if lastErr != nil {
		logger.Error(lastErr, "Cluster health check failed from all leader nodes")
	}
	return false
}

// checkClusterHealth performs a single cluster health check against a specific pod
func checkClusterHealth(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, podName string) error {
	logger := log.FromContext(ctx)

	cmd := []string{"redis-cli", "--cluster", "check", fmt.Sprintf("127.0.0.1:%d", *cr.Spec.Port)}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(ctx, client, cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			return fmt.Errorf("error getting redis password: %w", err)
		}
		cmd = append(cmd, "-a", pass)
	}
	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, podName)...)

	out, err := executeCommand1(ctx, client, cr, cmd, podName)
	if err != nil {
		return fmt.Errorf("failed to execute cluster check command: %w", err)
	}

	// Check for the expected success indicators
	// [OK] xxx keys in xxx masters.
	// [OK] All nodes agree about slots configuration.
	// [OK] All 16384 slots covered.
	okCount := strings.Count(out, "[OK]")
	if okCount != 3 {
		logger.V(1).Info("Cluster health check output", "pod", podName, "okCount", okCount, "output", out)
		return fmt.Errorf("cluster health check failed: expected 3 [OK] messages, got %d", okCount)
	}

	// Additional check: ensure no [ERR] or [WARNING] in critical lines
	if strings.Contains(out, "[ERR]") {
		return fmt.Errorf("cluster health check found errors in output")
	}

	return nil
}

// UnhealthyNodesInCluster returns the number of unhealthy nodes in the cluster cr
func UnhealthyNodesInCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) (int, error) {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()
	clusterNodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, node := range clusterNodes {
		if nodeFailedOrDisconnected(node) {
			count++
		}
	}
	log.FromContext(ctx).V(1).Info("Number of failed nodes in cluster", "Failed Node Count", count)
	return count, nil
}

// ForgetStaleNodes evicts ghost nodes the cluster has flagged as failed or with
// no address - the entries left behind when a pod loses its identity after a
// restart. It issues CLUSTER FORGET for each stale node id on every expected
// pod: CLUSTER FORGET only blacklists a node for 60 seconds, so any pod that
// still knows the id would otherwise re-introduce it through gossip.
func ForgetStaleNodes(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	leaderClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer leaderClient.Close()
	nodes, err := clusterNodes(ctx, leaderClient)
	if err != nil {
		return err
	}
	staleIDs := staleNodeIDs(nodes)
	if len(staleIDs) == 0 {
		return nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Forgetting stale cluster nodes", "Count", len(staleIDs), "Nodes", staleIDs)

	leaderReplicas := cr.Spec.GetReplicaCounts("leader")
	followerReplicas := cr.Spec.GetReplicaCounts("follower")
	pods := make([]string, 0, leaderReplicas+followerReplicas)
	for i := int32(0); i < leaderReplicas; i++ {
		pods = append(pods, fmt.Sprintf("%s-leader-%d", cr.Name, i))
	}
	for i := int32(0); i < followerReplicas; i++ {
		pods = append(pods, fmt.Sprintf("%s-follower-%d", cr.Name, i))
	}

	for _, podName := range pods {
		podClient := configureRedisClient(ctx, client, cr, podName)
		for _, id := range staleIDs {
			// "Unknown node" simply means this pod already does not know the id,
			// which is the desired end state, so it is not an error.
			if err := podClient.ClusterForget(ctx, id).Err(); err != nil && !strings.Contains(err.Error(), "Unknown node") {
				logger.V(1).Error(err, "CLUSTER FORGET failed", "Pod", podName, "Node", id)
			}
		}
		podClient.Close()
	}
	return nil
}

// staleNodeIDs returns the ids of nodes the cluster considers stale (flagged
// failed or with no address), excluding the node we are querying from.
func staleNodeIDs(nodes []clusterNodesResponse) []string {
	var ids []string
	for _, node := range nodes {
		if nodeIsStale(node) {
			ids = append(ids, node[0])
		}
	}
	return ids
}

// ReintegrateIsolatedFollowers re-attaches follower pods that came up isolated -
// reporting cluster_known_nodes == 1 because they lost their identity on restart
// and only know themselves. Such a pod cannot simply be re-added: it can claim
// bogus slots/data, which makes re-integration fail with "node is not empty". So
// each isolated follower is RESET + flushed clean, MET back into the cluster, and
// REPLICATEd to its shard's current master.
//
// It only acts against a reference cluster (leader-0) that is healthy and already
// covers all 16384 slots, which guarantees the isolated follower's data is
// redundant before it is flushed, and avoids interfering during initial
// cluster formation.
func ReintegrateIsolatedFollowers(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	leaderClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer leaderClient.Close()

	info, err := leaderClient.ClusterInfo(ctx).Result()
	if err != nil {
		return err
	}
	ci := parseClusterInfo(info)
	knownNodes, _ := strconv.Atoi(ci["cluster_known_nodes"])
	if ci["cluster_state"] != "ok" || ci["cluster_slots_assigned"] != "16384" || knownNodes <= 1 {
		return nil
	}

	nodes, err := clusterNodes(ctx, leaderClient)
	if err != nil {
		return err
	}
	leaderIP := getRedisServerIP(ctx, client, RedisDetails{PodName: cr.Name + "-leader-0", Namespace: cr.Namespace})
	if leaderIP == "" {
		return nil
	}
	port := strconv.Itoa(*cr.Spec.Port)

	leaderReplicas := cr.Spec.GetReplicaCounts("leader")
	followerReplicas := cr.Spec.GetReplicaCounts("follower")
	for i := int32(0); i < followerReplicas; i++ {
		podName := fmt.Sprintf("%s-follower-%d", cr.Name, i)
		leaderPod := fmt.Sprintf("%s-leader-%d", cr.Name, i%leaderReplicas)
		reintegrateIsolatedFollower(ctx, client, cr, podName, masterIDForShard(nodes, leaderPod), leaderIP, port)
	}
	return nil
}

// reintegrateIsolatedFollower resets, re-meets and re-replicates a single
// follower pod, but only if it is actually isolated (known_nodes == 1).
func reintegrateIsolatedFollower(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, podName, masterID, leaderIP, port string) {
	logger := log.FromContext(ctx)
	podClient := configureRedisClient(ctx, client, cr, podName)
	defer podClient.Close()

	info, err := podClient.ClusterInfo(ctx).Result()
	if err != nil {
		return
	}
	if parseClusterInfo(info)["cluster_known_nodes"] != "1" {
		return // not isolated
	}
	if masterID == "" {
		logger.V(1).Info("Could not determine shard master for isolated follower; skipping", "Pod", podName)
		return
	}

	logger.Info("Re-integrating isolated follower", "Pod", podName, "Master", masterID)
	// Make it a clean, empty node. Its data is redundant - the reference cluster
	// already covers all 16384 slots - and a replica must be empty to attach.
	if err := podClient.Do(ctx, "CLUSTER", "RESET", "SOFT").Err(); err != nil {
		logger.V(1).Error(err, "CLUSTER RESET failed", "Pod", podName)
	}
	if err := podClient.FlushAll(ctx).Err(); err != nil {
		logger.V(1).Error(err, "FLUSHALL failed", "Pod", podName)
	}
	if err := podClient.Do(ctx, "CLUSTER", "MEET", leaderIP, port).Err(); err != nil {
		logger.V(1).Error(err, "CLUSTER MEET failed", "Pod", podName)
		return
	}
	// REPLICATE can race the gossip propagation of the master id after MEET, so
	// retry until the new node knows its master.
	if err := retry.Do(
		func() error { return podClient.Do(ctx, "CLUSTER", "REPLICATE", masterID).Err() },
		retry.Attempts(5), retry.Delay(2*time.Second),
	); err != nil {
		logger.V(1).Error(err, "CLUSTER REPLICATE failed", "Pod", podName, "Master", masterID)
	}
}

// masterIDForShard returns the node id of the master serving the same shard as
// the given leader pod. If that leader pod is itself a master its own id is
// returned; if it has failed over and is now a replica, the id of the master it
// follows is returned. Returns "" if the pod is not found.
func masterIDForShard(nodes []clusterNodesResponse, leaderPodName string) string {
	for _, node := range nodes {
		if len(node) < 4 {
			continue
		}
		// node[1] is "<ip:port@bus>,<hostname>"; match the hostname exactly so
		// "leader-1" does not match "leader-10".
		comma := strings.LastIndex(node[1], ",")
		if comma < 0 || node[1][comma+1:] != leaderPodName {
			continue
		}
		if nodeIsOfType(node, "master") {
			return node[0]
		}
		if node[3] != "-" { // replica: node[3] is the id of its master
			return node[3]
		}
	}
	return ""
}

// FixInvertedLeaderRoles promotes any leader pod that has failed over and is now
// a replica back to master via CLUSTER FAILOVER. The operator's index-based
// scale-down assumes leader-i pods are masters, so a lingering inversion breaks
// it. Note this is a deliberate trade-off: re-promoting means a legitimate
// failover is followed by a corrective one, so it is gated behind
// ClusterSelfHealing rather than always-on.
//
// It only acts on a healthy, fully-covered, stable cluster (no failed nodes), so
// it never forces a failover during an outage or transition.
func FixInvertedLeaderRoles(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	leaderClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer leaderClient.Close()

	info, err := leaderClient.ClusterInfo(ctx).Result()
	if err != nil {
		return err
	}
	ci := parseClusterInfo(info)
	if ci["cluster_state"] != "ok" || ci["cluster_slots_assigned"] != "16384" {
		return nil
	}
	nodes, err := clusterNodes(ctx, leaderClient)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if nodeIsFailed(node) {
			return nil // topology unstable; don't realign roles now
		}
	}

	leaderReplicas := cr.Spec.GetReplicaCounts("leader")
	logger := log.FromContext(ctx)
	for i := int32(0); i < leaderReplicas; i++ {
		leaderPod := fmt.Sprintf("%s-leader-%d", cr.Name, i)
		if !leaderPodIsReplica(nodes, leaderPod) {
			continue
		}
		logger.Info("Leader pod has failed over to a replica; promoting it back via CLUSTER FAILOVER", "Pod", leaderPod)
		if err := ClusterFailover(ctx, client, cr, i); err != nil {
			logger.Error(err, "CLUSTER FAILOVER failed", "Pod", leaderPod)
		}
	}
	return nil
}

// leaderPodIsReplica reports whether the given leader pod currently appears as a
// replica in CLUSTER NODES - an "inverted" role (it failed over). Returns false
// if it is a master or not found.
func leaderPodIsReplica(nodes []clusterNodesResponse, leaderPodName string) bool {
	for _, node := range nodes {
		if len(node) < 3 {
			continue
		}
		comma := strings.LastIndex(node[1], ",")
		if comma < 0 || node[1][comma+1:] != leaderPodName {
			continue
		}
		return nodeIsOfType(node, "slave")
	}
	return false
}

func nodeIsOfType(node clusterNodesResponse, nodeType string) bool {
	return strings.Contains(node[2], nodeType)
}

// nodeIsFailed reports whether the cluster has flagged the node as failed. The
// flags field carries "fail" once the cluster agrees a node is down (and
// "fail?"/pfail while it is suspected), so a substring match covers both.
func nodeIsFailed(node clusterNodesResponse) bool {
	return strings.Contains(node[2], "fail")
}

// nodeIsStale reports whether a node is a ghost the cluster wants gone: flagged
// failed ("fail"/pfail) or with no known address ("noaddr"), and not the node we
// are querying from ("myself").
func nodeIsStale(node clusterNodesResponse) bool {
	if strings.Contains(node[2], "myself") {
		return false
	}
	return strings.Contains(node[2], "fail") || strings.Contains(node[2], "noaddr")
}

func nodeFailedOrDisconnected(node clusterNodesResponse) bool {
	return strings.Contains(node[2], "fail") || strings.Contains(node[7], "disconnected")
}

// configureRedisClient will configure the Redis Client
func configureRedisClient(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, podName string) *redis.Client {
	redisInfo := RedisDetails{
		PodName:   podName,
		Namespace: cr.Namespace,
	}
	var err error
	var pass string
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err = getRedisPassword(ctx, client, cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			log.FromContext(ctx).Error(err, "Error in getting redis password")
		}
	}
	opts := &redis.Options{
		Addr:     getRedisServerAddress(ctx, client, redisInfo, *cr.Spec.Port),
		Password: pass,
		DB:       0,
	}
	if cr.Spec.TLS != nil {
		opts.TLSConfig = getRedisTLSConfig(ctx, client, cr.Namespace, cr.Spec.TLS.Secret.SecretName, redisInfo.PodName)
	}
	return redis.NewClient(opts)
}

// executeCommand will execute the commands in pod
func executeCommand(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, cmd []string, podName string) {
	execOut, execErr := executeCommand1(ctx, client, cr, cmd, podName)
	if execErr != nil {
		log.FromContext(ctx).Error(execErr, "Could not execute command", "Command", cmd, "Output", execOut)
		return
	}
	log.FromContext(ctx).V(1).Info("Successfully executed the command", "Command", cmd, "Output", execOut)
}

func executeCommand1(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, cmd []string, podName string) (stdout string, stderr error) {
	var (
		execOut bytes.Buffer
		execErr bytes.Buffer
	)
	config, err := GenerateK8sConfig()()
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not find pod to execute")
		return "", err
	}
	targetContainer, pod := getContainerID(ctx, client, cr, podName)
	if targetContainer < 0 {
		log.FromContext(ctx).Error(err, "Could not find pod to execute")
		return "", err
	}

	req := client.CoreV1().RESTClient().Post().Resource("pods").Name(podName).Namespace(cr.Namespace).SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: pod.Spec.Containers[targetContainer].Name,
		Command:   cmd,
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to init executor")
		return "", err
	}

	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &execOut,
		Stderr: &execErr,
		Tty:    false,
	})
	if err != nil {
		return execOut.String(), fmt.Errorf("execute command with error: %w, stderr: %s", err, execErr.String())
	}
	return execOut.String(), nil
}

// getContainerID will return the id of container from pod
func getContainerID(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, podName string) (int, *corev1.Pod) {
	pod, err := client.CoreV1().Pods(cr.Namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not get pod info", "Pod Name", podName, "Namespace", cr.Namespace)
		return -1, nil
	}

	log.FromContext(ctx).V(1).Info("Pod info retrieved successfully", "Pod Name", podName, "Namespace", cr.Namespace)

	targetContainer := -1
	for containerID, tr := range pod.Spec.Containers {
		log.FromContext(ctx).V(1).Info("Inspecting container", "Pod Name", podName, "Container ID", containerID, "Container Name", tr.Name)
		if tr.Name == cr.Name+"-leader" {
			targetContainer = containerID
			log.FromContext(ctx).V(1).Info("Leader container found", "Container ID", containerID, "Container Name", tr.Name)
			break
		}
	}

	if targetContainer == -1 {
		log.FromContext(ctx).V(1).Info("Leader container not found in pod", "Pod Name", podName)
		return -1, nil
	}

	return targetContainer, pod
}

// checkRedisNodePresence will check if the redis node exist in cluster or not
func checkRedisNodePresence(ctx context.Context, nodeList []clusterNodesResponse, nodeName string) bool {
	log.FromContext(ctx).V(1).Info("Checking if Node is in cluster", "Node", nodeName)
	for _, node := range nodeList {
		s := strings.Split(node[1], ":")
		if s[0] == nodeName {
			return true
		}
	}
	return false
}

// configureRedisClient will configure the Redis Client
func configureRedisReplicationClient(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, podName string) *redis.Client {
	pod, err := client.CoreV1().Pods(cr.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil {
		return configureRedisReplicationClientForPod(ctx, client, cr, pod)
	}

	log.FromContext(ctx).V(1).Info("Falling back to redis replication pod lookup during client configuration", "pod", podName, "error", err)
	return configureRedisReplicationClientForAddress(ctx, client, cr, RedisDetails{
		PodName:   podName,
		Namespace: cr.Namespace,
	}, "")
}

func configureRedisReplicationClientForPod(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, pod *corev1.Pod) *redis.Client {
	return configureRedisReplicationClientForAddress(ctx, client, cr, RedisDetails{
		PodName:   pod.Name,
		Namespace: cr.Namespace,
	}, pod.Status.PodIP)
}

func configureRedisReplicationClientForAddress(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, redisInfo RedisDetails, podIP string) *redis.Client {
	var err error
	var pass string
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err = getRedisPassword(ctx, client, cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			log.FromContext(ctx).Error(err, "Error in getting redis password")
		}
	}
	var addr string
	if cr.Spec.TLS != nil {
		// Use DNS name for TLS connections
		addr = fmt.Sprintf("%s:%d", getRedisReplicationHostname(redisInfo, cr), 6379)
	} else {
		if podIP == "" {
			podIP = getRedisServerIP(ctx, client, redisInfo)
		}
		addr = formatRedisAddress(podIP, 6379)
	}
	opts := &redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       0,
	}
	if cr.Spec.TLS != nil {
		opts.TLSConfig = getRedisTLSConfig(ctx, client, cr.Namespace, cr.Spec.TLS.Secret.SecretName, redisInfo.PodName)
	}
	return redis.NewClient(opts)
}

func formatRedisAddress(ip string, port int) string {
	if ip == "" {
		return fmt.Sprintf("%s:%d", ip, port)
	}
	format := "%s:%d"
	if net.ParseIP(ip).To4() == nil {
		format = "[%s]:%d"
	}
	return fmt.Sprintf(format, ip, port)
}

func getRedisReplicationHostname(redisInfo RedisDetails, cr *rrvb2.RedisReplication) string {
	return fmt.Sprintf("%s.%s-headless.%s.svc.%s", redisInfo.PodName, cr.Name, cr.Namespace, envs.GetServiceDNSDomain())
}

// Get Redis nodes by it's role i.e. master, slave and sentinel
func GetRedisNodesByRole(ctx context.Context, cl kubernetes.Interface, cr *rrvb2.RedisReplication, redisRole string) ([]string, error) {
	return getRedisNodesByRole(ctx, cl, cr, redisRole, func(ctx context.Context, pod *corev1.Pod) (string, error) {
		redisClient := configureRedisReplicationClientForPod(ctx, cl, cr, pod)
		defer redisClient.Close()

		return checkRedisServerRole(ctx, redisClient, pod.Name)
	})
}

func getRedisNodesByRole(ctx context.Context, cl kubernetes.Interface, cr *rrvb2.RedisReplication, redisRole string, probeRole func(context.Context, *corev1.Pod) (string, error)) ([]string, error) {
	statefulset, err := GetStatefulSet(ctx, cl, cr.GetNamespace(), cr.GetName())
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to Get the Statefulset of the", "custom resource", cr.Name, "in namespace", cr.Namespace)
		return nil, err
	}

	var pods []string
	replicas := cr.Spec.GetReplicationCounts("replication")

	for i := 0; i < int(replicas); i++ {
		podName := statefulset.Name + "-" + strconv.Itoa(i)
		pod, err := cl.CoreV1().Pods(cr.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}

		if !IsRedisPodProbeable(pod) {
			continue
		}

		podRole, err := probeRole(ctx, pod)
		if err != nil {
			return nil, err
		}
		if podRole == redisRole {
			pods = append(pods, podName)
		}
	}

	return pods, nil
}

func IsRedisPodProbeable(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// Check the Redis Server Role i.e. master, slave and sentinel
func checkRedisServerRole(ctx context.Context, redisClient *redis.Client, podName string) (string, error) {
	info, err := redisClient.Info(ctx, "Replication").Result()
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to Get the role Info of the", "redis pod", podName)
		return "", err
	}
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "role:") {
			role := strings.TrimPrefix(line, "role:")
			log.FromContext(ctx).V(1).Info("Role of the Redis Pod", "pod", podName, "role", role)
			return role, nil
		}
	}
	log.FromContext(ctx).Error(err, "Failed to find role from Info # Replication in", "redis pod", podName)
	return "", err
}

// checkAttachedSlave would return redis pod name which has slave
func checkAttachedSlave(ctx context.Context, redisClient *redis.Client, podName string) int {
	info, err := redisClient.Info(ctx, "Replication").Result()
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get the connected slaves count of the", "redis pod", podName)
		return -1 // return -1 if failed to get the connected slaves count
	}

	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "connected_slaves:") {
			var connected_slaves int
			connected_slaves, err = strconv.Atoi(strings.TrimPrefix(line, "connected_slaves:"))
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to convert the connected slaves count of the", "redis pod", podName)
				return -1
			}
			log.FromContext(ctx).V(1).Info("Connected Slaves of the Redis Pod", "pod", podName, "connected_slaves", connected_slaves)
			return connected_slaves
		}
	}

	log.FromContext(ctx).Error(nil, "Failed to find connected_slaves from Info # Replication in", "redis pod", podName)
	return 0
}

func CreateMasterSlaveReplication(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, masterPods []string, realMasterPod string) error {
	log.FromContext(ctx).V(1).Info("Redis Master Node is set to", "pod", realMasterPod)
	realMasterInfo := RedisDetails{
		PodName:   realMasterPod,
		Namespace: cr.Namespace,
	}

	var realMasterAddr string
	if cr.Spec.TLS != nil {
		// Use DNS name for TLS connections to match certificate validation
		realMasterAddr = getRedisReplicationHostname(realMasterInfo, cr)
		log.FromContext(ctx).V(1).Info("Using DNS address for TLS master replication", "masterAddr", realMasterAddr)
	} else {
		// Use IP address for non-TLS connections
		realMasterPodIP := getRedisServerIP(ctx, client, realMasterInfo)
		if realMasterPodIP == "" {
			return errors.New("CreateMasterSlaveReplication got empty master IP, refusing")
		}
		realMasterAddr = realMasterPodIP
		log.FromContext(ctx).V(1).Info("Using IP address for non-TLS master replication", "masterAddr", realMasterAddr)
	}

	for i := 0; i < len(masterPods); i++ {
		if masterPods[i] != realMasterPod {
			redisClient := configureRedisReplicationClient(ctx, client, cr, masterPods[i])
			defer redisClient.Close()
			log.FromContext(ctx).V(1).Info("Setting the", "pod", masterPods[i], "to slave of", realMasterPod, "masterAddr", realMasterAddr)
			err := redisClient.SlaveOf(ctx, realMasterAddr, "6379").Err()
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to set", "pod", masterPods[i], "to slave of", realMasterPod, "masterAddr", realMasterAddr)
				return err
			}
		}
	}

	return nil
}

func GetRedisReplicationRealMaster(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, masterPods []string) string {
	for _, podName := range masterPods {
		redisClient := configureRedisReplicationClient(ctx, client, cr, podName)
		defer redisClient.Close()

		if checkAttachedSlave(ctx, redisClient, podName) > 0 {
			return podName
		}
	}
	return ""
}

// SetRedisClusterDynamicConfig applies dynamic configuration to each Redis instance in the cluster
func SetRedisClusterDynamicConfig(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	// Get dynamic configuration
	dynamicConfig := cr.Spec.GetRedisDynamicConfig()
	if len(dynamicConfig) == 0 {
		return nil
	}

	// Get the number of leader and follower pods
	leaderReplicas := cr.Spec.GetReplicaCounts("leader")
	followerReplicas := cr.Spec.GetReplicaCounts("follower")

	// Apply configuration to each Redis instance
	for i := 0; i < int(leaderReplicas+followerReplicas); i++ {
		var podName string
		if i < int(leaderReplicas) {
			podName = cr.Name + "-leader-" + strconv.Itoa(i)
		} else {
			podName = cr.Name + "-follower-" + strconv.Itoa(i-int(leaderReplicas))
		}

		redisClient := configureRedisClient(ctx, client, cr, podName)
		defer redisClient.Close()

		// Check if Redis instance is accessible
		pong, err := redisClient.Ping(ctx).Result()
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to ping Redis instance", "pod", podName)
			continue
		}
		if pong != "PONG" {
			log.FromContext(ctx).V(1).Info("Redis instance not ready", "pod", podName)
			continue
		}

		// Apply dynamic configuration parameters
		for _, config := range dynamicConfig {
			parts := strings.SplitN(config, " ", 2)
			if len(parts) != 2 {
				log.FromContext(ctx).Error(nil, "Invalid config format", "config", config)
				continue
			}

			err := redisClient.ConfigSet(ctx, parts[0], parts[1]).Err()
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to set config",
					"key", parts[0],
					"value", parts[1],
					"pod", podName)
				return err
			}

			log.FromContext(ctx).V(1).Info("Successfully set config",
				"key", parts[0],
				"value", parts[1],
				"pod", podName)
		}
	}

	return nil
}
