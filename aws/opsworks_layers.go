package aws

import (
	"fmt"
	"log"
	"strconv"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/opsworks"
	"github.com/hashicorp/errwrap"
	"os"
)

// OpsWorks has a single concept of "layer" which represents several different
// layer types. The differences between these are in some extra properties that
// get packed into an "Attributes" map, but in the OpsWorks UI these are presented
// as first-class options, and so Terraform prefers to expose them this way and
// hide the implementation detail that they are all packed into a single type
// in the underlying API.
//
// This file contains utilities that are shared between all of the concrete
// layer resource types, which have names matching aws_opsworks_*_layer .

type opsworksLayerTypeAttribute struct {
	AttrName  string
	Type      schema.ValueType
	Default   interface{}
	Required  bool
	WriteOnly bool
}

type opsworksLayerType struct {
	TypeName         string
	DefaultLayerName string
	Attributes       map[string]*opsworksLayerTypeAttribute
	CustomShortName  bool
}

var (
	opsworksTrueString  = "true"
	opsworksFalseString = "false"
)

func (lt *opsworksLayerType) SchemaResource() *schema.Resource {
	resourceSchema := map[string]*schema.Schema{
		"id": &schema.Schema{
			Type:     schema.TypeString,
			Computed: true,
		},

		"auto_assign_elastic_ips": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  false,
		},

		"auto_assign_public_ips": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  false,
		},

		"custom_instance_profile_arn": &schema.Schema{
			Type:     schema.TypeString,
			Optional: true,
		},

		"elastic_load_balancer": &schema.Schema{
			Type:     schema.TypeString,
			Optional: true,
		},

		"custom_setup_recipes": &schema.Schema{
			Type:     schema.TypeList,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
		},

		"custom_configure_recipes": &schema.Schema{
			Type:     schema.TypeList,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
		},

		"custom_deploy_recipes": &schema.Schema{
			Type:     schema.TypeList,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
		},

		"custom_undeploy_recipes": &schema.Schema{
			Type:     schema.TypeList,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
		},

		"custom_shutdown_recipes": &schema.Schema{
			Type:     schema.TypeList,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
		},

		"custom_security_group_ids": &schema.Schema{
			Type:     schema.TypeSet,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
			Set:      schema.HashString,
		},

		"custom_json": &schema.Schema{
			Type:      schema.TypeString,
			StateFunc: normalizeJson,
			Optional:  true,
		},

		"auto_healing": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  true,
		},

		"install_updates_on_boot": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  true,
		},

		"instance_shutdown_timeout": &schema.Schema{
			Type:     schema.TypeInt,
			Optional: true,
			Default:  120,
		},

		"drain_elb_on_shutdown": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  true,
		},

		"system_packages": &schema.Schema{
			Type:     schema.TypeSet,
			Optional: true,
			Elem:     &schema.Schema{Type: schema.TypeString},
			Set:      schema.HashString,
		},

		"stack_id": &schema.Schema{
			Type:     schema.TypeString,
			ForceNew: true,
			Required: true,
		},

		"use_ebs_optimized_instances": &schema.Schema{
			Type:     schema.TypeBool,
			Optional: true,
			Default:  false,
		},

		"ebs_volume": &schema.Schema{
			Type:     schema.TypeSet,
			Optional: true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{

					"iops": &schema.Schema{
						Type:     schema.TypeInt,
						Optional: true,
						Default:  0,
					},

					"mount_point": &schema.Schema{
						Type:     schema.TypeString,
						Required: true,
					},

					"number_of_disks": &schema.Schema{
						Type:     schema.TypeInt,
						Required: true,
					},

					"raid_level": &schema.Schema{
						Type:     schema.TypeString,
						Optional: true,
						Default:  "",
					},

					"size": &schema.Schema{
						Type:     schema.TypeInt,
						Required: true,
					},

					"type": &schema.Schema{
						Type:     schema.TypeString,
						Optional: true,
						Default:  "standard",
					},
				},
			},
			Set: func(v interface{}) int {
				m := v.(map[string]interface{})
				return hashcode.String(m["mount_point"].(string))
			},
		},

		"layer_endpoint": {
			Type:     schema.TypeString,
			Computed: true,
		},
	}

	if lt.CustomShortName {
		resourceSchema["short_name"] = &schema.Schema{
			Type:     schema.TypeString,
			Required: true,
		}
	}

	if lt.DefaultLayerName != "" {
		resourceSchema["name"] = &schema.Schema{
			Type:     schema.TypeString,
			Optional: true,
			Default:  lt.DefaultLayerName,
		}
	} else {
		resourceSchema["name"] = &schema.Schema{
			Type:     schema.TypeString,
			Required: true,
		}
	}

	for key, def := range lt.Attributes {
		resourceSchema[key] = &schema.Schema{
			Type:     def.Type,
			Default:  def.Default,
			Required: def.Required,
			Optional: !def.Required,
		}
	}

	return &schema.Resource{
		Read: func(d *schema.ResourceData, meta interface{}) error {

			return lt.Read(d, meta)
		},
		Create: func(d *schema.ResourceData, meta interface{}) error {

			return lt.Create(d, meta)
		},
		Update: func(d *schema.ResourceData, meta interface{}) error {

			return lt.Update(d, meta)
		},
		Delete: func(d *schema.ResourceData, meta interface{}) error {

			return lt.Delete(d, meta)
		},
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: resourceSchema,
	}
}

func opsworksConnRegion(region string, meta interface{}) (*opsworks.OpsWorks, error) {
	originalConn := meta.(*AWSClient).opsworksconn

	// Regions are the same, no need to reconfigure
	if originalConn.Config.Region != nil && *originalConn.Config.Region == region {
		return originalConn, nil
	}

	// Set up base session
	sess, err := session.NewSession(&originalConn.Config)
	if err != nil {
		return nil, errwrap.Wrapf("Error creating AWS session: {{err}}", err)
	}

	sess.Handlers.Build.PushBackNamed(addTerraformVersionToUserAgent)

	if extraDebug := os.Getenv("TERRAFORM_AWS_AUTHFAILURE_DEBUG"); extraDebug != "" {
		sess.Handlers.UnmarshalError.PushFrontNamed(debugAuthFailure)
	}

	newSession := sess.Copy(&aws.Config{Region: aws.String(region)})
	newOpsworksconn := opsworks.New(newSession)

	log.Printf("[DEBUG] Returning new OpsWorks client")
	return newOpsworksconn, nil
}

func (lt *opsworksLayerType) Read(d *schema.ResourceData, meta interface{}) error {

	client := meta.(*AWSClient).opsworksconn

	var conErr error
	if v := d.Get("layer_endpoint").(string); v != "" {
		client, conErr = opsworksConnRegion(v, meta)
		if conErr != nil {
			return conErr
		}
	}

	req := &opsworks.DescribeLayersInput{
		LayerIds: []*string{
			aws.String(d.Id()),
		},
	}

	log.Printf("[DEBUG] Reading OpsWorks layer: %s", d.Id())

	/*resp, err := client.DescribeLayers(req)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			if awserr.Code() == "ResourceNotFoundException" {
				d.SetId("")
				return nil
			}
		}
		return err
	}*/
	var notFound int
	var resp *opsworks.DescribeLayersOutput
	var dErr error

	for {
		resp, dErr = client.DescribeLayers(req)
		if dErr != nil {
			if awserr, ok := dErr.(awserr.Error); ok {
				if awserr.Code() == "ResourceNotFoundException" {
					if notFound < 1 {
						// If we haven't already, try us-east-1, legacy connection
						notFound++
						var connErr error
						client, connErr = opsworksConnRegion("us-east-1", meta)
						if connErr != nil {
							return connErr
						}
						// start again from the top of the FOR loop, but with a client
						// configured to talk to us-east-1
						continue
					}

					// We've tried both the original and us-east-1 endpoint, and the layer
					// is still not found
					log.Printf("[DEBUG] OpsWorks layer (%s) not found", d.Id())
					d.SetId("")
					return nil
				}
				// not ResoureNotFoundException, fall through to returning error
			}
			return dErr
		}
		// If the layer was found, set the layer_endpoint
		if client.Config.Region != nil && *client.Config.Region != "" {
			log.Printf("[DEBUG] Setting layer for (%s) to (%s)", d.Id(), *client.Config.Region)
			if err := d.Set("layer_endpoint", *client.Config.Region); err != nil {
				log.Printf("[WARN] Error setting layer_endpoint: %s", err)
			}
		}
		log.Printf("[DEBUG] Breaking layer endpoint search, found layer for (%s)", d.Id())
		// Break the FOR loop
		break
	}

	layer := resp.Layers[0]
	d.Set("id", layer.LayerId)
	d.Set("auto_assign_elastic_ips", layer.AutoAssignElasticIps)
	d.Set("auto_assign_public_ips", layer.AutoAssignPublicIps)
	d.Set("custom_instance_profile_arn", layer.CustomInstanceProfileArn)
	d.Set("custom_security_group_ids", flattenStringList(layer.CustomSecurityGroupIds))
	d.Set("auto_healing", layer.EnableAutoHealing)
	d.Set("install_updates_on_boot", layer.InstallUpdatesOnBoot)
	d.Set("name", layer.Name)
	d.Set("system_packages", flattenStringList(layer.Packages))
	d.Set("stack_id", layer.StackId)
	d.Set("use_ebs_optimized_instances", layer.UseEbsOptimizedInstances)

	if lt.CustomShortName {
		d.Set("short_name", layer.Shortname)
	}

	if v := layer.CustomJson; v == nil {
		if err := d.Set("custom_json", ""); err != nil {
			return err
		}
	} else if err := d.Set("custom_json", normalizeJson(*v)); err != nil {
		return err
	}

	lt.SetAttributeMap(d, layer.Attributes)
	lt.SetLifecycleEventConfiguration(d, layer.LifecycleEventConfiguration)
	lt.SetCustomRecipes(d, layer.CustomRecipes)
	lt.SetVolumeConfigurations(d, layer.VolumeConfigurations)

	/* get ELB */
	ebsRequest := &opsworks.DescribeElasticLoadBalancersInput{
		LayerIds: []*string{
			aws.String(d.Id()),
		},
	}
	loadBalancers, err := client.DescribeElasticLoadBalancers(ebsRequest)
	if err != nil {
		return err
	}

	if loadBalancers.ElasticLoadBalancers == nil || len(loadBalancers.ElasticLoadBalancers) == 0 {
		d.Set("elastic_load_balancer", "")
	} else {
		loadBalancer := loadBalancers.ElasticLoadBalancers[0]
		if loadBalancer != nil {
			d.Set("elastic_load_balancer", loadBalancer.ElasticLoadBalancerName)
		}
	}

	return nil
}

func (lt *opsworksLayerType) Create(d *schema.ResourceData, meta interface{}) error {

	client := meta.(*AWSClient).opsworksconn

	req := &opsworks.CreateLayerInput{
		AutoAssignElasticIps:        aws.Bool(d.Get("auto_assign_elastic_ips").(bool)),
		AutoAssignPublicIps:         aws.Bool(d.Get("auto_assign_public_ips").(bool)),
		CustomInstanceProfileArn:    aws.String(d.Get("custom_instance_profile_arn").(string)),
		CustomRecipes:               lt.CustomRecipes(d),
		CustomSecurityGroupIds:      expandStringSet(d.Get("custom_security_group_ids").(*schema.Set)),
		EnableAutoHealing:           aws.Bool(d.Get("auto_healing").(bool)),
		InstallUpdatesOnBoot:        aws.Bool(d.Get("install_updates_on_boot").(bool)),
		LifecycleEventConfiguration: lt.LifecycleEventConfiguration(d),
		Name:                     aws.String(d.Get("name").(string)),
		Packages:                 expandStringSet(d.Get("system_packages").(*schema.Set)),
		Type:                     aws.String(lt.TypeName),
		StackId:                  aws.String(d.Get("stack_id").(string)),
		UseEbsOptimizedInstances: aws.Bool(d.Get("use_ebs_optimized_instances").(bool)),
		Attributes:               lt.AttributeMap(d),
		VolumeConfigurations:     lt.VolumeConfigurations(d),
	}

	if lt.CustomShortName {
		req.Shortname = aws.String(d.Get("short_name").(string))
	} else {
		req.Shortname = aws.String(lt.TypeName)
	}

	req.CustomJson = aws.String(d.Get("custom_json").(string))

	log.Printf("[DEBUG] Creating OpsWorks layer: %s", d.Id())

	resp, err := client.CreateLayer(req)
	if err != nil {
		return err
	}

	layerId := *resp.LayerId
	d.SetId(layerId)
	d.Set("id", layerId)

	loadBalancer := aws.String(d.Get("elastic_load_balancer").(string))
	if loadBalancer != nil && *loadBalancer != "" {
		log.Printf("[DEBUG] Attaching load balancer: %s", *loadBalancer)
		_, err := client.AttachElasticLoadBalancer(&opsworks.AttachElasticLoadBalancerInput{
			ElasticLoadBalancerName: loadBalancer,
			LayerId:                 &layerId,
		})
		if err != nil {
			return err
		}
	}

	return lt.Read(d, meta)
}

func (lt *opsworksLayerType) Update(d *schema.ResourceData, meta interface{}) error {

	client := meta.(*AWSClient).opsworksconn
	var conErr error
	if v := d.Get("layer_endpoint").(string); v != "" {
		client, conErr = opsworksConnRegion(v, meta)
		if conErr != nil {
			return conErr
		}
	}

	req := &opsworks.UpdateLayerInput{
		LayerId:                     aws.String(d.Id()),
		AutoAssignElasticIps:        aws.Bool(d.Get("auto_assign_elastic_ips").(bool)),
		AutoAssignPublicIps:         aws.Bool(d.Get("auto_assign_public_ips").(bool)),
		CustomInstanceProfileArn:    aws.String(d.Get("custom_instance_profile_arn").(string)),
		CustomRecipes:               lt.CustomRecipes(d),
		CustomSecurityGroupIds:      expandStringSet(d.Get("custom_security_group_ids").(*schema.Set)),
		EnableAutoHealing:           aws.Bool(d.Get("auto_healing").(bool)),
		InstallUpdatesOnBoot:        aws.Bool(d.Get("install_updates_on_boot").(bool)),
		LifecycleEventConfiguration: lt.LifecycleEventConfiguration(d),
		Name:                     aws.String(d.Get("name").(string)),
		Packages:                 expandStringSet(d.Get("system_packages").(*schema.Set)),
		UseEbsOptimizedInstances: aws.Bool(d.Get("use_ebs_optimized_instances").(bool)),
		Attributes:               lt.AttributeMap(d),
		VolumeConfigurations:     lt.VolumeConfigurations(d),
	}

	if lt.CustomShortName {
		req.Shortname = aws.String(d.Get("short_name").(string))
	} else {
		req.Shortname = aws.String(lt.TypeName)
	}

	req.CustomJson = aws.String(d.Get("custom_json").(string))

	log.Printf("[DEBUG] Updating OpsWorks layer: %s", d.Id())

	if d.HasChange("elastic_load_balancer") {
		lbo, lbn := d.GetChange("elastic_load_balancer")

		loadBalancerOld := aws.String(lbo.(string))
		loadBalancerNew := aws.String(lbn.(string))

		if loadBalancerOld != nil && *loadBalancerOld != "" {
			log.Printf("[DEBUG] Dettaching load balancer: %s", *loadBalancerOld)
			_, err := client.DetachElasticLoadBalancer(&opsworks.DetachElasticLoadBalancerInput{
				ElasticLoadBalancerName: loadBalancerOld,
				LayerId:                 aws.String(d.Id()),
			})
			if err != nil {
				return err
			}
		}

		if loadBalancerNew != nil && *loadBalancerNew != "" {
			log.Printf("[DEBUG] Attaching load balancer: %s", *loadBalancerNew)
			_, err := client.AttachElasticLoadBalancer(&opsworks.AttachElasticLoadBalancerInput{
				ElasticLoadBalancerName: loadBalancerNew,
				LayerId:                 aws.String(d.Id()),
			})
			if err != nil {
				return err
			}
		}
	}

	_, err := client.UpdateLayer(req)
	if err != nil {
		return err
	}

	return lt.Read(d, meta)
}

func (lt *opsworksLayerType) Delete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*AWSClient).opsworksconn
	var conErr error
	if v := d.Get("layer_endpoint").(string); v != "" {
		client, conErr = opsworksConnRegion(v, meta)
		if conErr != nil {
			return conErr
		}
	}

	req := &opsworks.DeleteLayerInput{
		LayerId: aws.String(d.Id()),
	}

	log.Printf("[DEBUG] Deleting OpsWorks layer: %s", d.Id())

	_, err := client.DeleteLayer(req)
	return err
}

func (lt *opsworksLayerType) AttributeMap(d *schema.ResourceData) map[string]*string {
	attrs := map[string]*string{}

	for key, def := range lt.Attributes {
		value := d.Get(key)
		switch def.Type {
		case schema.TypeString:
			strValue := value.(string)
			attrs[def.AttrName] = &strValue
		case schema.TypeInt:
			intValue := value.(int)
			strValue := strconv.Itoa(intValue)
			attrs[def.AttrName] = &strValue
		case schema.TypeBool:
			boolValue := value.(bool)
			if boolValue {
				attrs[def.AttrName] = &opsworksTrueString
			} else {
				attrs[def.AttrName] = &opsworksFalseString
			}
		default:
			// should never happen
			panic(fmt.Errorf("Unsupported OpsWorks layer attribute type"))
		}
	}

	return attrs
}

func (lt *opsworksLayerType) SetAttributeMap(d *schema.ResourceData, attrs map[string]*string) {
	for key, def := range lt.Attributes {
		// Ignore write-only attributes; we'll just keep what we already have stored.
		// (The AWS API returns garbage placeholder values for these.)
		if def.WriteOnly {
			continue
		}

		if strPtr, ok := attrs[def.AttrName]; ok && strPtr != nil {
			strValue := *strPtr

			switch def.Type {
			case schema.TypeString:
				d.Set(key, strValue)
			case schema.TypeInt:
				intValue, err := strconv.Atoi(strValue)
				if err == nil {
					d.Set(key, intValue)
				} else {
					// Got garbage from the AWS API
					d.Set(key, nil)
				}
			case schema.TypeBool:
				boolValue := true
				if strValue == opsworksFalseString {
					boolValue = false
				}
				d.Set(key, boolValue)
			default:
				// should never happen
				panic(fmt.Errorf("Unsupported OpsWorks layer attribute type"))
			}
			return

		} else {
			d.Set(key, nil)
		}
	}
}

func (lt *opsworksLayerType) LifecycleEventConfiguration(d *schema.ResourceData) *opsworks.LifecycleEventConfiguration {
	return &opsworks.LifecycleEventConfiguration{
		Shutdown: &opsworks.ShutdownEventConfiguration{
			DelayUntilElbConnectionsDrained: aws.Bool(d.Get("drain_elb_on_shutdown").(bool)),
			ExecutionTimeout:                aws.Int64(int64(d.Get("instance_shutdown_timeout").(int))),
		},
	}
}

func (lt *opsworksLayerType) SetLifecycleEventConfiguration(d *schema.ResourceData, v *opsworks.LifecycleEventConfiguration) {
	if v == nil || v.Shutdown == nil {
		d.Set("drain_elb_on_shutdown", nil)
		d.Set("instance_shutdown_timeout", nil)
	} else {
		d.Set("drain_elb_on_shutdown", v.Shutdown.DelayUntilElbConnectionsDrained)
		d.Set("instance_shutdown_timeout", v.Shutdown.ExecutionTimeout)
	}
}

func (lt *opsworksLayerType) CustomRecipes(d *schema.ResourceData) *opsworks.Recipes {
	return &opsworks.Recipes{
		Configure: expandStringList(d.Get("custom_configure_recipes").([]interface{})),
		Deploy:    expandStringList(d.Get("custom_deploy_recipes").([]interface{})),
		Setup:     expandStringList(d.Get("custom_setup_recipes").([]interface{})),
		Shutdown:  expandStringList(d.Get("custom_shutdown_recipes").([]interface{})),
		Undeploy:  expandStringList(d.Get("custom_undeploy_recipes").([]interface{})),
	}
}

func (lt *opsworksLayerType) SetCustomRecipes(d *schema.ResourceData, v *opsworks.Recipes) {
	// Null out everything first, and then we'll consider what to put back.
	d.Set("custom_configure_recipes", nil)
	d.Set("custom_deploy_recipes", nil)
	d.Set("custom_setup_recipes", nil)
	d.Set("custom_shutdown_recipes", nil)
	d.Set("custom_undeploy_recipes", nil)

	if v == nil {
		return
	}

	d.Set("custom_configure_recipes", flattenStringList(v.Configure))
	d.Set("custom_deploy_recipes", flattenStringList(v.Deploy))
	d.Set("custom_setup_recipes", flattenStringList(v.Setup))
	d.Set("custom_shutdown_recipes", flattenStringList(v.Shutdown))
	d.Set("custom_undeploy_recipes", flattenStringList(v.Undeploy))
}

func (lt *opsworksLayerType) VolumeConfigurations(d *schema.ResourceData) []*opsworks.VolumeConfiguration {
	configuredVolumes := d.Get("ebs_volume").(*schema.Set).List()
	result := make([]*opsworks.VolumeConfiguration, len(configuredVolumes))

	for i := 0; i < len(configuredVolumes); i++ {
		volumeData := configuredVolumes[i].(map[string]interface{})

		result[i] = &opsworks.VolumeConfiguration{
			MountPoint:    aws.String(volumeData["mount_point"].(string)),
			NumberOfDisks: aws.Int64(int64(volumeData["number_of_disks"].(int))),
			Size:          aws.Int64(int64(volumeData["size"].(int))),
			VolumeType:    aws.String(volumeData["type"].(string)),
		}
		iops := int64(volumeData["iops"].(int))
		if iops != 0 {
			result[i].Iops = aws.Int64(iops)
		}

		raidLevelStr := volumeData["raid_level"].(string)
		if raidLevelStr != "" {
			raidLevel, err := strconv.Atoi(raidLevelStr)
			if err == nil {
				result[i].RaidLevel = aws.Int64(int64(raidLevel))
			}
		}
	}

	return result
}

func (lt *opsworksLayerType) SetVolumeConfigurations(d *schema.ResourceData, v []*opsworks.VolumeConfiguration) {
	newValue := make([]*map[string]interface{}, len(v))

	for i := 0; i < len(v); i++ {
		config := v[i]
		data := make(map[string]interface{})
		newValue[i] = &data

		if config.Iops != nil {
			data["iops"] = int(*config.Iops)
		} else {
			data["iops"] = 0
		}
		if config.MountPoint != nil {
			data["mount_point"] = *config.MountPoint
		}
		if config.NumberOfDisks != nil {
			data["number_of_disks"] = int(*config.NumberOfDisks)
		}
		if config.RaidLevel != nil {
			data["raid_level"] = strconv.Itoa(int(*config.RaidLevel))
		}
		if config.Size != nil {
			data["size"] = int(*config.Size)
		}
		if config.VolumeType != nil {
			data["type"] = *config.VolumeType
		}
	}

	d.Set("ebs_volume", newValue)
}
