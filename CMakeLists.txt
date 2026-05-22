cmake_minimum_required(VERSION 3.10)
project(nc-bridge-cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Поиск Boost (компонент thread в новых версиях часто встроен в headers-only, 
# но для надежности оставим system)
find_package(Boost REQUIRED COMPONENTS system)

add_executable(nc_bridge_cpp main.cpp)

# Подключаем заголовки и линкуем библиотеки
target_link_libraries(nc_bridge_cpp 
    PRIVATE 
    Boost::system
)

# Для Linux добавляем pthread, для Windows — системные библиотеки сокетов
if(UNIX)
    target_link_libraries(nc_bridge_cpp PRIVATE pthread)
else()
    # Windows требует ws2_32 и mswsock для работы с сетью в Boost.Asio
    target_link_libraries(nc_bridge_cpp PRIVATE ws2_32 mswsock)
endif()
message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
