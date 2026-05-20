# Указываем минимальную версию CMake
cmake_minimum_required(VERSION 3.10)

# Название проекта
project(nc-bridge-cpp)

# Устанавливаем стандарт C++ (Boost.Beast требует минимум C++11, но лучше 14 или 17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Поиск пакета Boost
# Нам нужны компоненты system (базовый) и thread
find_package(Boost REQUIRED COMPONENTS system thread)

# Проверка: если Boost найден, добавляем пути к заголовкам
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()

# Создаем исполняемый файл из исходника main.cpp
add_executable(nc_bridge_cpp main.cpp)

# Привязываем библиотеки Boost и системную библиотеку потоков pthread
target_link_libraries(nc_bridge_cpp 
    ${Boost_LIBRARIES}
    pthread
)

# Дополнительно: выводим сообщение о статусе для отладки
message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
