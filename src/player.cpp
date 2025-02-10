//* ROS
#include <ros/ros.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/PointCloud2.h>
#include <rosgraph_msgs/Clock.h>
#include <cv_bridge/cv_bridge.h>

//* OpenCV
#include <opencv2/core.hpp>
#include <opencv2/imgproc.hpp>
#include <opencv2/highgui.hpp>

//* PCL
#include <pcl_ros/point_cloud.h>
#include <pcl_conversions/pcl_conversions.h>
#include <pcl/point_types.h>

//* Google Log
#include <glog/logging.h>
#include <gflags/gflags.h>

//* CPP Standard
#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <yaml-cpp/yaml.h>

//* Project
#include "datathread.hpp"

struct SensorTopic {
    std::string sensor;
    std::string topic;
};

// Load Meta Data
bool loadMetadata(const std::string &file_path, std::vector<SensorTopic> &topics) {
    std::ifstream file(file_path, std::ifstream::binary);
    if (!file.is_open()) {
        ROS_ERROR("Failed to open JSON config file: %s", file_path.c_str());
        return false;
    }

    topics.clear();

    auto metadata = YAML::LoadFile(file_path);
    // Camera
    if (YAML::Node parameter = metadata["camera"]) {
        for (const auto &topic_name : parameter.as<std::vector<std::string>>()) {
            topics.push_back({"camera", topic_name});
        }
    }
    // LiDAR
    if (YAML::Node parameter = metadata["lidar"]) {
        for (const auto &topic_name : parameter.as<std::vector<std::string>>()) {
            topics.push_back({"lidar", topic_name});
        }
    }
    // IMU
    if (YAML::Node parameter = metadata["imu"]) {
        for (const auto &topic_name : parameter.as<std::vector<std::string>>()) {
            topics.push_back({"imu", topic_name});
        }
    }

    for (const auto &topic : topics) {
        ROS_INFO("Loaded topic: %s (%s)",topic.topic.c_str(), topic.sensor.c_str());
    }
    return true;
}

class Player {
public:
    Player() {}
    ~Player() {}

    void ready() {
        dthread.active = true;
        dthread.thread = std::thread(&Player::funcThread, this);
    }

    void push(int64_t timestamp) {
        dthread.push(timestamp);
    }

    void notify_all() {
        dthread.cv.notify_all();
    }

protected:
    virtual void funcThread()=0;

public:
    std::vector<int64_t> timestamps;

protected:
    std::string topic_name;

    // ROS Variables
    std::shared_ptr<ros::NodeHandle> nh;
    ros::Publisher publisher;

    // Thread
    DataThread<int64_t> dthread;
};

class CameraPlayer : public Player {
public:
    CameraPlayer(std::shared_ptr<ros::NodeHandle> nh, const std::string &topic_name, const std::string &dir_path) {
        this->nh = nh;
        this->topic_name = topic_name;
        this->seek = 0;
        
        publisher = nh->advertise<sensor_msgs::Image>(topic_name, 10);

        for (const auto &entry : std::filesystem::directory_iterator(dir_path)) {
            if (entry.is_regular_file()) {
                auto filename = entry.path().filename().string();
                std::string type = filename.substr(filename.size()-4);
                CHECK(type==".png" || type=="jpg");
                int64_t timsestamp = std::stoll(filename.substr(0, filename.size()-4));
                timestamps.push_back(timsestamp);
                img_paths[timsestamp] = entry.path().string();
            }
        }
        CHECK(timestamps.size() > 0 && timestamps.size() == img_paths.size());
        std::sort(timestamps.begin(), timestamps.end());

        img = cv::imread(img_paths[timestamps[seek]], cv::IMREAD_UNCHANGED);
        CHECK(!img.empty());
    }
    ~CameraPlayer() {}

private:
    void funcThread() override {
        while (true) {
            // std::cout<<"loop\n";
            std::unique_lock<std::mutex> ul(dthread.mutex);
            dthread.cv.wait(ul);
            if(dthread.active == false) return;
            ul.unlock();

            while(!dthread.data_queue.empty()){
                auto timestamp = dthread.pop();
                CHECK(timestamp == timestamps[seek]);

                // Timer::start("cam process");
                // Publish current image    
                cv_bridge::CvImage img_msg;
                img_msg.header.stamp.fromNSec(timestamp);
                img_msg.header.frame_id = "world";
                img_msg.encoding = sensor_msgs::image_encodings::BGR8;
                img_msg.image = img;
                publisher.publish(img_msg.toImageMsg());
                // Timer::check("cam process", "pub");

                // Load next image
                if (seek+1 < timestamps.size()) {
                    img = cv::imread(img_paths[timestamps[seek+1]], cv::IMREAD_UNCHANGED);
                    CHECK(!img.empty());
                    seek++;
                } else {
                    break;
                }
                // Timer::check("cam process", "read next");
            }
            if(dthread.active == false) return;
        }
    }

private:
    // Data
    std::map<int64_t, std::string> img_paths;
    int seek;
    cv::Mat img;
};

class LidarPlayer : public Player {
public:
    LidarPlayer(std::shared_ptr<ros::NodeHandle> nh, const std::string &topic_name, const std::string &dir_path) {
        this->nh = nh;
        this->topic_name = topic_name;
        this->seek = 0;
        
        publisher = nh->advertise<sensor_msgs::PointCloud2>(topic_name, 10);

        for (const auto &entry : std::filesystem::directory_iterator(dir_path)) {
            if (entry.is_regular_file()) {
                auto filename = entry.path().filename().string();
                std::string type = filename.substr(filename.size()-4);
                CHECK(type == ".bin");
                int64_t timsestamp = std::stoll(filename.substr(0, filename.size()-4));
                timestamps.push_back(timsestamp);
                bin_paths[timsestamp] = entry.path().string();
            }
        }
        CHECK(timestamps.size() > 0 && timestamps.size() == bin_paths.size());
        std::sort(timestamps.begin(), timestamps.end());

        pcl::PointCloud<pcl::PointXYZI> cloud;
        cloud.clear();
        std::ifstream file;
        file.open(bin_paths[timestamps[seek]], std::ios::in | std::ios::binary);
        while(!file.eof()){
            pcl::PointXYZI point;
            file.read(reinterpret_cast<char *>(&point.x), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.y), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.z), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.intensity), sizeof(float));
            cloud.points.push_back (point);
        }
        file.close();
        pcl::toROSMsg(cloud, cloud_msg);
    }
    ~LidarPlayer() {}

private:
    void funcThread() override {
        while (true) {
            // std::cout<<"loop\n";
            std::unique_lock<std::mutex> ul(dthread.mutex);
            dthread.cv.wait(ul);
            if(dthread.active == false) return;
            ul.unlock();

            while(!dthread.data_queue.empty()){
                auto timestamp = dthread.pop();
                CHECK(timestamp == timestamps[seek]);

                // Timer::start("lidar process");
                // Publish current cloud
                cloud_msg.header.stamp.fromNSec(timestamp);
                cloud_msg.header.frame_id = "lidar";
                publisher.publish(cloud_msg);
                // Timer::check("lidar process", "pub");

                // Load next cloud
                if (seek+1 < timestamps.size()) {
                    pcl::PointCloud<pcl::PointXYZI> cloud;
                    cloud.clear();
                    std::ifstream file;
                    file.open(bin_paths[timestamps[seek+1]], std::ios::in | std::ios::binary);
                    while(!file.eof()){
                        pcl::PointXYZI point;
                        file.read(reinterpret_cast<char *>(&point.x), sizeof(float));
                        file.read(reinterpret_cast<char *>(&point.y), sizeof(float));
                        file.read(reinterpret_cast<char *>(&point.z), sizeof(float));
                        file.read(reinterpret_cast<char *>(&point.intensity), sizeof(float));
                        cloud.points.push_back (point);
                    }
                    file.close();
                    pcl::toROSMsg(cloud, cloud_msg);
                    seek++;
                } else {
                    break;
                }
                // Timer::check("lidar process", "read next");
            }
            if(dthread.active == false) return;
        }
    }

private:
    // Data
    std::map<int64_t, std::string> bin_paths;
    int seek;
    sensor_msgs::PointCloud2 cloud_msg;
};


class ImuPlayer : public Player {
public:
    ImuPlayer(std::shared_ptr<ros::NodeHandle> nh, const std::string &topic_name, const std::string &path) {
        this->nh = nh;
        this->topic_name = topic_name;
        this->seek = 0;
        
        publisher = nh->advertise<sensor_msgs::Imu>(topic_name, 10);

        std::string file_path = path + ".csv";
        std::ifstream file(file_path);
        CHECK(file.is_open());

        std::string line;
        while (std::getline(file, line)) {
            if (line[0] == '#') continue;

            std::stringstream ss(line);
            std::vector<double> values;
            std::string value;
            while (std::getline(ss, value, ',')) {
                values.push_back(std::stod(value));
            }
            if (values.size() < 11) continue;

            sensor_msgs::Imu msg;
            int64_t timestamp = values[0];
            msg.header.stamp.fromNSec(timestamp);
            msg.orientation.x = values[1];
            msg.orientation.y = values[2];
            msg.orientation.z = values[3];
            msg.orientation.w = values[4];
            msg.angular_velocity.x = values[8];
            msg.angular_velocity.y = values[9];
            msg.angular_velocity.z = values[10];
            msg.linear_acceleration.x = values[11];
            msg.linear_acceleration.y = values[12];
            msg.linear_acceleration.z = values[13];

            timestamps.push_back(timestamp);
            imu_msgs.insert({timestamp, msg});
        }
    }
    ~ImuPlayer() {}

private:
    void funcThread() override {
        while (true) {
            // std::cout<<"loop\n";
            std::unique_lock<std::mutex> ul(dthread.mutex);
            dthread.cv.wait(ul);
            if(dthread.active == false) return;
            ul.unlock();

            while(!dthread.data_queue.empty()){
                auto timestamp = dthread.pop();
                CHECK(timestamp == timestamps[seek]);

                // Publish current cloud
                publisher.publish(imu_msgs[timestamp]);

                if (seek+1 < timestamps.size()) {
                    seek++;
                } else {
                    break;
                }
            }
            if(dthread.active == false) return;
        }
    }

private:
    // Data
    std::map<int64_t, sensor_msgs::Imu> imu_msgs;
    int seek;
};



std::mutex data_mutex;
std::multimap<int64_t, std::string> data_stamps;
std::unordered_map<std::string, std::shared_ptr<Player>> data_players;

bool play_flag = false;

int64_t processed_stamp = 0;
int64_t pre_timer_stamp = 0;
double play_rate = 1.0;
std::mutex timer_mutex;

void TimerCallback(const ros::TimerEvent& event) {
    int64_t current_stamp = ros::Time::now().toNSec();
    if(play_flag == true){
      processed_stamp += static_cast<int64_t>(static_cast<double>(current_stamp - pre_timer_stamp) * play_rate);
    }
    pre_timer_stamp = current_stamp;
}

void DataStampThread() {
    int64_t initial_stamp = data_stamps.begin()->first - 1;
    int64_t end_stamp = data_stamps.rbegin()->first;

    for(const auto &node : data_stamps){
        auto timestamp = node.first;
        auto topic = node.second;
        // std::cout<<timestamp<<" "<<topic<<" \n";

        while(timestamp > (initial_stamp + processed_stamp)){
            usleep(1);
            //wait for data publish
        }

        int progress = 50 * (timestamp - initial_stamp) / (end_stamp - initial_stamp);
        // std::stringstream sstream;
        std::cout << std::fixed;
        std::cout.precision(5);
        std::cout << "\r[";
        for (int i=0; i<50; ++i) {
            if (i <= progress) std::cout << "=";
            else std::cout << "-";
        }
        std::cout << "] ";
        std::cout << "(" << (double)timestamp/1e9 << " / " << (double)end_stamp/1e9 << ")" << std::flush;
        
        data_players[topic]->push(timestamp);
        data_players[topic]->notify_all();
    }
    std::cout << "Data publish complete\n";
}

// 📌 Main function
int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, (char ***)&argv, true);
    FLAGS_logtostderr = 1;
    FLAGS_minloglevel = google::INFO;

    ros::init(argc, argv, "data_player");
    std::string metadata_path = "";

    auto nh = std::make_shared<ros::NodeHandle>("~");
    nh->param<std::string>("metadata", metadata_path, metadata_path);

    std::vector<SensorTopic> topics;
    if (!loadMetadata(metadata_path, topics)) return -1;
    std::string dataset_dir = std::filesystem::path(metadata_path).parent_path().string();

    //* Sort all data
    for (const auto &topic : topics) {
        std::cout<<"Load data of "<<topic.topic<<"... \n";
        if (topic.sensor == "camera") {
            std::shared_ptr<CameraPlayer> player = std::make_shared<CameraPlayer>(
                nh, topic.topic, dataset_dir+topic.topic
            );
            data_players[topic.topic] = player;
            for (auto timestamp : player->timestamps) {
                data_stamps.insert({timestamp, topic.topic});
            }
        } else if (topic.sensor == "lidar") {
            std::shared_ptr<LidarPlayer> player = std::make_shared<LidarPlayer>(
                nh, topic.topic, dataset_dir+topic.topic
            );
            data_players[topic.topic] = player;
            for (auto timestamp : player->timestamps) {
                data_stamps.insert({timestamp, topic.topic});
            }
        } else if (topic.sensor == "imu") {
            std::shared_ptr<ImuPlayer> player = std::make_shared<ImuPlayer>(
                nh, topic.topic, dataset_dir+topic.topic
            );
            data_players[topic.topic] = player;
            for (auto timestamp : player->timestamps) {
                data_stamps.insert({timestamp, topic.topic});
            }
        } 
    }

    //* Start Publish
    play_flag = true;
    pre_timer_stamp = ros::Time::now().toNSec();
    ros::Timer timer = nh->createTimer(ros::Duration(0.0001), boost::bind(&TimerCallback, _1));
    
    std::thread data_stamp_thread = std::thread(DataStampThread);
    for (const auto &node : data_players) {
        node.second->ready();
    }
    
    ros::AsyncSpinner spinner(0);
    spinner.start();
    ros::waitForShutdown();

    std::cout<<"\n>>End Process<<\n";

    return 0;
}
