cmake_minimum_required(VERSION 3.0.2)
project(data_player)

add_compile_options(-std=c++17)

find_package(catkin REQUIRED COMPONENTS 
    roscpp
    rosbag
    tf
    std_msgs
    sensor_msgs
    geometry_msgs
    nav_msgs
    image_transport
    cv_bridge
)
find_package(glog REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(Eigen3 REQUIRED)
find_package(OpenCV REQUIRED)
find_package(PCL REQUIRED)
find_package(OpenGL REQUIRED)
find_package(Iridescence REQUIRED)

###################################
## catkin specific configuration ##
###################################
## The catkin_package macro generates cmake config files for your package
## Declare things to be passed to dependent projects
## INCLUDE_DIRS: uncomment this if your package contains header files
## LIBRARIES: libraries you create in this project that dependent projects also need
## CATKIN_DEPENDS: catkin_packages dependent projects also need
## DEPENDS: system dependencies of this project that dependent projects also need
catkin_package(
    CATKIN_DEPENDS roscpp rosbag tf std_msgs geometry_msgs sensor_msgs nav_msgs image_transport cv_bridge
    INCLUDE_DIRS src
)

###########
## Build ##
###########

## Specify additional locations of header files
## Your package locations should be listed before other locations
include_directories(
    src
    include
    ${catkin_INCLUDE_DIRS}
    ${YAML_CPP_INCLUDE_DIR}
    ${EIGEN3_INCLUDE_DIR}
    
    ${PCL_INCLUDE_DIRS}
    ${OPENGL_INCLUDE_DIR}

)

## Declare a C++ executable
## With catkin_make all packages are built within a single CMake context
## The recommended prefix ensures that target names across packages don't collide
add_executable(${PROJECT_NAME}_node 
    # test/test_ipm.cpp
    src/player.cpp
)

## Specify libraries to link a library or executable target against
target_link_libraries(${PROJECT_NAME}_node
    ${catkin_LIBRARIES}
    glog::glog
    gflags
    ${YAML_CPP_LIBRARIES}
    
    ${OpenCV_LIBS}
    ${PCL_LIBRARIES}
    ${OPENGL_LIBRARIES}
    Iridescence::Iridescence
)
