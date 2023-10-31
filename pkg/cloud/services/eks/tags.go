/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package eks

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/eks"
	"github.com/aws/aws-sdk-go/service/eks/eksiface"
	"github.com/pkg/errors"
	"k8s.io/utils/pointer"

	infrav1 "sigs.k8s.io/cluster-api-provider-aws/v2/api/v1beta2"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/converters"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/tags"
)

const (
	eksClusterNameTag              = "eks:cluster-name"
	eksNodeGroupNameTag            = "eks:nodegroup-name"
	eksClusterAutoscalerEnabledTag = "k8s.io/cluster-autoscaler/enabled"
)

func (s *Service) reconcileTags(cluster *eks.Cluster) error {
	clusterTags := converters.MapPtrToMap(cluster.Tags)
	buildParams := s.getEKSTagParams(*cluster.Arn)
	tagsBuilder := tags.New(buildParams, tags.WithEKS(s.EKSClient))
	if err := tagsBuilder.Ensure(clusterTags); err != nil {
		return fmt.Errorf("failed ensuring tags on cluster: %w", err)
	}

	return nil
}

func (s *Service) getEKSTagParams(id string) *infrav1.BuildParams {
	name := s.scope.KubernetesClusterName()

	return &infrav1.BuildParams{
		ClusterName: name,
		ResourceID:  id,
		Lifecycle:   infrav1.ResourceLifecycleOwned,
		Name:        aws.String(name),
		Role:        aws.String(infrav1.CommonRoleTagValue),
		Additional:  s.scope.AdditionalTags(),
	}
}

func getTagUpdates(currentTags map[string]string, tags map[string]string) (untagKeys []string, newTags map[string]string) {
	untagKeys = []string{}
	newTags = make(map[string]string)
	for key := range currentTags {
		if _, ok := tags[key]; !ok {
			untagKeys = append(untagKeys, key)
		}
	}
	for key, value := range tags {
		if currentV, ok := currentTags[key]; !ok || value != currentV {
			newTags[key] = value
		}
	}
	return untagKeys, newTags
}

func getASGTagUpdates(clusterName string, currentTags map[string]string, tags map[string]string) (tagsToDelete map[string]string, tagsToAdd map[string]string) {
	officialASGTagsByEKS := []string{
		eksClusterNameTag,
		eksNodeGroupNameTag,
		fmt.Sprintf("k8s.io/cluster-autoscaler/%s", clusterName),
		eksClusterAutoscalerEnabledTag,
		infrav1.ClusterAWSCloudProviderTagKey(clusterName),
	}
	tagsToDelete = make(map[string]string)
	tagsToAdd = make(map[string]string)
	for k, v := range currentTags {
		if _, ok := tags[k]; !ok {
			isOfficialTag := false
			for _, tag := range officialASGTagsByEKS {
				if tag == k {
					isOfficialTag = true
					break
				}
			}
			if !isOfficialTag {
				tagsToDelete[k] = v
			}
		}
	}
	for key, value := range tags {
		if currentV, ok := currentTags[key]; !ok || value != currentV {
			tagsToAdd[key] = value
		}
	}
	return tagsToDelete, tagsToAdd
}

func (s *NodegroupService) reconcileTags(ng *eks.Nodegroup) error {
	tags := ngTags(s.scope.ClusterName(), s.scope.AdditionalTags())
	if err := updateTags(s.EKSClient, ng.NodegroupArn, aws.StringValueMap(ng.Tags), tags); err != nil {
		return err
	}
	return s.reconcileInstanceTags(ng)
}

func (s *NodegroupService) reconcileInstanceTags(ng *eks.Nodegroup) error {
	ngtags := ngTags(s.scope.ClusterName(), s.scope.AdditionalTags())
	groupReq := autoscaling.DescribeAutoScalingGroupsInput{}
	for _, asg := range ng.Resources.AutoScalingGroups {
		groupReq.AutoScalingGroupNames = append(groupReq.AutoScalingGroupNames, asg.Name)
	}
	groups, err := s.AutoscalingClient.DescribeAutoScalingGroups(&groupReq)
	if err != nil {
		return errors.Wrap(err, "failed to describe AutoScalingGroup for nodegroup")
	}
	ids := make([]*string, 0)
	for _, group := range groups.AutoScalingGroups {
		for _, instance := range group.Instances {
			ids = append(ids, instance.InstanceId)
		}
	}
	s.scope.Info("instances of autoscaling groups", "count", len(ids), "service", "tags:NodegroupService")
	ec2Req := ec2.DescribeInstancesInput{InstanceIds: ids}
	output, err := s.EC2Client.DescribeInstances(&ec2Req)
	if err != nil {
		return errors.Wrap(err, "failed to describe Instances")
	}

	for {
		for _, reservation := range output.Reservations {
			for _, instance := range reservation.Instances {
				tags, desired := make(map[string]string), make(map[string]string)
				for _, tag := range instance.Tags {
					if tag != nil && tag.Key != nil && tag.Value != nil {
						tags[*tag.Key] = *tag.Value
						desired[*tag.Key] = *tag.Value
					}
				}
				for k, v := range ngtags {
					desired[k] = v
				}
				s.scope.Info("updating instance tag", "instance", instance.InstanceId)
				if err = updateECSTags(s.EC2Client, []*string{instance.InstanceId}, tags, desired); err != nil {
					return err
				}
				volumeIds := make([]*string, 0)
				for _, b := range instance.BlockDeviceMappings {
					if b != nil && b.Ebs != nil && b.Ebs.VolumeId != nil {
						volumeIds = append(volumeIds, b.Ebs.VolumeId)
					}
				}
				if err = s.reconcileEBSVolumeTags(volumeIds, ng); err != nil {
					return err
				}
			}
		}
		if output.NextToken == nil {
			break
		}
	}
	return nil
}

func (s *NodegroupService) reconcileEBSVolumeTags(volumeIds []*string, ng *eks.Nodegroup) error {
	ngtags := ngTags(s.scope.ClusterName(), s.scope.AdditionalTags())
	req := ec2.DescribeVolumesInput{VolumeIds: volumeIds}
	output, err := s.EC2Client.DescribeVolumes(&req)
	if err != nil {
		return errors.Wrap(err, "failed to describe Volumes")
	}
	for {
		for _, volume := range output.Volumes {
			tags, desired := make(map[string]string), make(map[string]string)
			desired[eksClusterNameTag] = s.scope.ClusterName()
			desired[eksNodeGroupNameTag] = *ng.NodegroupName
			for _, tag := range volume.Tags {
				if tag != nil && tag.Key != nil && tag.Value != nil {
					tags[*tag.Key] = *tag.Value
					desired[*tag.Key] = *tag.Value
				}
			}
			for k, v := range ngtags {
				desired[k] = v
			}
			if err = updateECSTags(s.EC2Client, []*string{volume.VolumeId}, tags, desired); err != nil {
				return err
			}
		}
		if output.NextToken == nil {
			break
		}
	}
	return nil
}

func tagDescriptionsToMap(input []*autoscaling.TagDescription) map[string]string {
	tags := make(map[string]string)
	for _, v := range input {
		tags[*v.Key] = *v.Value
	}
	return tags
}

func (s *NodegroupService) reconcileASGTags(ng *eks.Nodegroup) error {
	s.scope.Info("Reconciling ASG tags", "cluster-name", s.scope.ClusterName(), "nodegroup-name", *ng.NodegroupName)
	asg, err := s.describeASGs(ng)
	if err != nil {
		return errors.Wrap(err, "failed to describe ASG for nodegroup")
	}

	tagsToDelete, tagsToAdd := getASGTagUpdates(s.scope.ClusterName(), tagDescriptionsToMap(asg.Tags), s.scope.AdditionalTags())
	s.scope.Debug("Tags", "tagsToAdd", tagsToAdd, "tagsToDelete", tagsToDelete)

	if len(tagsToAdd) > 0 {
		input := &autoscaling.CreateOrUpdateTagsInput{}
		for k, v := range tagsToAdd {
			// The k/vCopy is used to address the "Implicit memory aliasing in for loop" issue
			// https://stackoverflow.com/questions/62446118/implicit-memory-aliasing-in-for-loop
			kCopy := k
			vCopy := v
			input.Tags = append(input.Tags, &autoscaling.Tag{
				Key:               &kCopy,
				PropagateAtLaunch: aws.Bool(true),
				ResourceId:        asg.AutoScalingGroupName,
				ResourceType:      pointer.String("auto-scaling-group"),
				Value:             &vCopy,
			})
		}
		_, err = s.AutoscalingClient.CreateOrUpdateTags(input)
		if err != nil {
			return errors.Wrap(err, "failed to add tags to nodegroup's AutoScalingGroup")
		}
	}

	if len(tagsToDelete) > 0 {
		input := &autoscaling.DeleteTagsInput{}
		for k := range tagsToDelete {
			// The k/vCopy is used to address the "Implicit memory aliasing in for loop" issue
			// https://stackoverflow.com/questions/62446118/implicit-memory-aliasing-in-for-loop
			kCopy := k
			input.Tags = append(input.Tags, &autoscaling.Tag{
				Key:          &kCopy,
				ResourceId:   asg.AutoScalingGroupName,
				ResourceType: pointer.String("auto-scaling-group"),
			})
		}
		_, err = s.AutoscalingClient.DeleteTags(input)
		if err != nil {
			return errors.Wrap(err, "failed to delete tags to nodegroup's AutoScalingGroup")
		}
	}

	return nil
}

func (s *FargateService) reconcileTags(fp *eks.FargateProfile) error {
	tags := ngTags(s.scope.ClusterName(), s.scope.AdditionalTags())
	return updateTags(s.EKSClient, fp.FargateProfileArn, aws.StringValueMap(fp.Tags), tags)
}

func updateTags(client eksiface.EKSAPI, arn *string, existingTags, desiredTags map[string]string) error {
	untagKeys, newTags := getTagUpdates(existingTags, desiredTags)

	if len(newTags) > 0 {
		tagInput := &eks.TagResourceInput{
			ResourceArn: arn,
			Tags:        aws.StringMap(newTags),
		}
		_, err := client.TagResource(tagInput)
		if err != nil {
			return err
		}
	}

	if len(untagKeys) > 0 {
		untagInput := &eks.UntagResourceInput{
			ResourceArn: arn,
			TagKeys:     aws.StringSlice(untagKeys),
		}
		_, err := client.UntagResource(untagInput)
		if err != nil {
			return err
		}
	}

	return nil
}

func updateECSTags(client ec2iface.EC2API, resources []*string, existingTags, desiredTags map[string]string) error {
	untagKeys, newTags := getTagUpdates(existingTags, desiredTags)
	if len(newTags) > 0 {
		tags := make([]*ec2.Tag, 0)
		for k, v := range newTags {
			tags = append(tags, &ec2.Tag{
				Key:   &k,
				Value: &v,
			})
		}
		tagInput := &ec2.CreateTagsInput{
			Tags:      tags,
			Resources: resources,
		}
		_, err := client.CreateTags(tagInput)
		if err != nil {
			return err
		}
	}

	if len(untagKeys) > 0 {
		tags := make([]*ec2.Tag, len(untagKeys))
		for i, k := range untagKeys {
			tags[i] = &ec2.Tag{Key: &k}
		}
		untagInput := &ec2.DeleteTagsInput{
			Resources: resources,
			Tags:      tags,
		}
		_, err := client.DeleteTags(untagInput)
		if err != nil {
			return err
		}
	}

	return nil
}
